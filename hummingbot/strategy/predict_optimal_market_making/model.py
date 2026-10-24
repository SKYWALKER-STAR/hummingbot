"""Feil--Nendel (2026), sections 2 and 4. NumPy/SciPy implementation.

All model times are fractions of an explicitly configured horizon. Intensities
are fixed-size-equivalent fills per horizon, not per second.
"""
from dataclasses import asdict, dataclass, replace
import hashlib
import json
from pathlib import Path

import numpy as np


@dataclass(frozen=True)
class ModelConfig:
    sigma0: float = .6
    sigma1: float = .3
    sigma2: float = .1
    eta: float = 3.
    a0: float = 100.
    a1: float = 150.
    xi: float = 2.
    nu: float = 1.
    k0: float = 35.
    k1: float = 50.
    kappa: float = 1.5
    gamma: float = .004
    gamma_terminal: float = .001
    q_min: float = -100.
    q_max: float = 100.
    trade_size: float = 10.
    time_steps: int = 160
    price_steps: int = 80
    quote_steps: int = 200
    p_min: float = .001
    tolerance: float = 1e-7
    max_iterations: int = 300

    def __post_init__(self):
        for key, value in asdict(self).items():
            if isinstance(value, bool) or not np.isfinite(value):
                raise ValueError(f"Invalid model field {key}")
        for key in ('time_steps', 'price_steps', 'quote_steps', 'max_iterations'):
            if type(getattr(self, key)) is not int or getattr(self, key) < 2:
                raise ValueError(f"{key} must be an integer >= 2")
        for key in ('sigma0', 'eta', 'xi', 'nu', 'k0', 'k1', 'kappa', 'trade_size', 'tolerance'):
            if getattr(self, key) <= 0:
                raise ValueError(f"{key} must be positive")
        if self.eta < 1 or max(self.xi, self.kappa) > 50:
            raise ValueError("eta >= 1 and xi/kappa <= 50 required")
        if min(self.sigma1, self.sigma2, self.a0, self.a1, self.gamma, self.gamma_terminal) < 0:
            raise ValueError("Negative coefficient")
        if not 0 < self.p_min < .5 or not self.q_min < self.q_max:
            raise ValueError("Invalid price/inventory bounds")
        n = (self.q_max - self.q_min) / self.trade_size
        if not np.isclose(n, round(n)) or n > 1000:
            raise ValueError("Inventory range must be an integer multiple of trade_size, <= 1000 steps")

    @property
    def version(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()[:16]


def volatility(c, t, p):
    x = np.log(p / (1 - p))
    return p * (1 - p) * (c.sigma0 + c.sigma1 * t ** c.eta + c.sigma2 / (1 + x * x))


def intensity(c, t, p, price, side):
    """Section 4.1 intensity, with zero intensity at the appropriate boundary."""
    activity = (c.a0 + c.a1 * np.expm1(c.xi * t) / np.expm1(c.xi)) * np.sqrt(p * (1 - p))
    k = c.k0 + (c.k1 - c.k0) * np.expm1(c.kappa * t) / np.expm1(c.kappa)
    if side == 'BUY':
        shape, offset = 2 * price / (price + p), p - price
    elif side == 'SELL':
        shape, offset = 2 * (1 - price) / (2 - price - p), price - p
    else:
        raise ValueError(side)
    return activity * shape ** c.nu * np.exp(np.clip(-k * offset, -700, 700))


class HJBTable:
    def __init__(self, config, values, iterations=()):
        self.config = config
        self.values = values
        self.times = np.linspace(0, 1, config.time_steps + 1)
        self.prices = np.linspace(config.p_min, 1 - config.p_min, config.price_steps + 1)
        self.inventories = np.linspace(config.q_min, config.q_max,
                                       round((config.q_max - config.q_min) / config.trade_size) + 1)
        self.iterations = tuple(iterations)

    @classmethod
    def solve(cls, c):
        from scipy.sparse import coo_matrix
        from scipy.sparse.linalg import spsolve
        table = cls(c, np.zeros((c.time_steps + 1, c.price_steps + 1,
                               round((c.q_max - c.q_min) / c.trade_size) + 1)))
        p, q = table.prices, table.inventories
        h, dp = 1 / c.time_steps, p[1] - p[0]
        table.values[-1] = -c.gamma_terminal * p[:, None] * (1 - p[:, None]) * q[None, :] ** 2
        candidates = np.linspace(0, 1, c.quote_steps + 1)
        iterations = []
        for n in range(c.time_steps - 1, -1, -1):
            t = table.times[n]
            var = volatility(c, t, p) ** 2
            r = .5 * h * var / dp ** 2
            lower, upper = -r[1:].copy(), -r[:-1].copy()
            # Homogeneous Neumann: reflected ghost cells at both truncated ends.
            upper[0] *= 2
            lower[-1] *= 2
            diagonal = 1 + 2 * r
            lambdas = {s: intensity(c, t, p[:, None], candidates[None, :], s) for s in ('BUY', 'SELL')}
            old = table.values[n + 1].copy()
            base = table.values[n + 1] - h * c.gamma * var[:, None] * q[None, :] ** 2
            # Howard policy iteration solves the SAME implicit nonlinear scheme
            # as section 4.2. Unlike naive Picard iteration it handles the very
            # high intensities of quotes crossing the model probability.
            nq = len(q)
            ids = np.arange(old.size).reshape(old.shape)
            for iteration in range(c.max_iterations):
                lb,la,reward = np.zeros_like(old),np.zeros_like(old),np.zeros_like(old)
                for side in ('BUY', 'SELL'):
                    z = ((old[:, :-1] - old[:, 1:]) if side == 'BUY'
                         else (old[:, 1:] - old[:, :-1])) / c.trade_size
                    margin = p[:, None] - candidates[None, :] if side == 'BUY' else candidates[None, :] - p[:, None]
                    # Loop inventory to avoid allocating a p*q*quote tensor.
                    for k in range(z.shape[1]):
                        score = lambdas[side] * (margin - z[:, k, None])
                        choice = np.argmax(score,axis=1)
                        active = score[np.arange(len(p)),choice] > 0
                        rate = lambdas[side][np.arange(len(p)),choice] * active
                        col = k if side == 'BUY' else k+1
                        (lb if side == 'BUY' else la)[:,col] = rate
                        reward[:,col] += c.trade_size*rate*margin[np.arange(len(p)),choice]
                row_indices = [ids.ravel(),ids[:-1,:].ravel(),ids[1:,:].ravel(),
                               ids[:,:-1].ravel(),ids[:,1:].ravel()]
                col_indices = [ids.ravel(),ids[1:,:].ravel(),ids[:-1,:].ravel(),
                               ids[:,1:].ravel(),ids[:,:-1].ravel()]
                entries = [(diagonal[:,None]+h*(lb+la)).ravel(),
                           np.repeat(upper,nq),np.repeat(lower,nq),
                           (-h*lb[:,:-1]).ravel(),(-h*la[:,1:]).ravel()]
                matrix = coo_matrix((np.concatenate(entries),(np.concatenate(row_indices),np.concatenate(col_indices))),
                                    shape=(old.size,old.size)).tocsc()
                new = spsolve(matrix,(base+h*reward).ravel()).reshape(old.shape)
                if not np.all(np.isfinite(new)):
                    raise ArithmeticError("Nonfinite HJB iterate")
                error = np.max(np.abs(new - old))
                old = new
                if error < c.tolerance:
                    break
            else:
                raise ArithmeticError(f"HJB did not converge at step {n}; increase time_steps")
            table.values[n] = new
            iterations.append(iteration + 1)
        table.iterations = tuple(iterations)
        return table

    def value(self, t, p, q):
        if not 0 <= t <= 1 or not self.config.q_min <= q <= self.config.q_max:
            raise ValueError("State outside HJB domain")
        if not self.config.p_min <= p <= 1 - self.config.p_min:
            raise ValueError("Price outside truncated HJB domain")
        indices, weights = [], []
        for grid, x in ((self.times, t), (self.prices, p), (self.inventories, q)):
            i = max(0, min(len(grid) - 2, int(np.searchsorted(grid, x, side='right') - 1)))
            indices.append(i)
            weights.append((x - grid[i]) / (grid[i + 1] - grid[i]))
        result = 0.
        for a in (0, 1):
            for b in (0, 1):
                for d in (0, 1):
                    result += (weights[0] if a else 1 - weights[0]) * (weights[1] if b else 1 - weights[1]) * (weights[2] if d else 1 - weights[2]) * self.values[indices[0]+a, indices[1]+b, indices[2]+d]
        return float(result)

    def quote(self, t, p, q, side, candidates):
        """Optimize on supplied legal quote ticks, allowing withdrawal (zero gain)."""
        if side not in ('BUY','SELL'):
            raise ValueError(side)
        c = self.config
        next_q = q + (c.trade_size if side == 'BUY' else -c.trade_size)
        if not c.q_min <= next_q <= c.q_max or not len(candidates):
            return None
        z = (self.value(t, p, q) - self.value(t, p, next_q)) / c.trade_size
        candidates = np.asarray(candidates)
        margin = p - candidates if side == 'BUY' else candidates - p
        score = intensity(c, t, p, candidates, side) * (margin - z)
        i = int(np.argmax(score))
        return float(candidates[i]) if np.isfinite(score[i]) and score[i] > 0 else None

    def value_many(self,t,p,q):
        """Vectorized trilinear interpolation for Monte Carlo paths."""
        p,q = np.asarray(p),np.asarray(q)
        if (not 0 <= t <= 1 or np.any(q < self.config.q_min) or np.any(q > self.config.q_max)
                or np.any(p < self.config.p_min) or np.any(p > 1-self.config.p_min)):
            raise ValueError('State outside HJB domain')
        indices,weights = [],[]
        for grid,x in ((self.times,np.full_like(p,t)),(self.prices,p),(self.inventories,q)):
            i = np.clip(np.searchsorted(grid,x,side='right')-1,0,len(grid)-2)
            indices.append(i)
            weights.append((x-grid[i])/(grid[i+1]-grid[i]))
        result = np.zeros_like(p,dtype=float)
        for a in (0,1):
            for b in (0,1):
                for d in (0,1):
                    result += (weights[0] if a else 1-weights[0])*(weights[1] if b else 1-weights[1])*(weights[2] if d else 1-weights[2])*self.values[indices[0]+a,indices[1]+b,indices[2]+d]
        return result

    def quote_many(self,t,p,q,side,candidates):
        if side not in ('BUY','SELL'):
            raise ValueError(side)
        c = self.config
        result = np.full(len(p),np.nan)
        for start in range(0,len(p),512):
            end = min(len(p),start+512)
            next_q = q[start:end]+(c.trade_size if side=='BUY' else -c.trade_size)
            mask = (next_q>=c.q_min)&(next_q<=c.q_max)
            ids = np.arange(start,end)[mask]
            if not len(ids):
                continue
            z = (self.value_many(t,p[ids],q[ids])-self.value_many(t,p[ids],next_q[mask]))/c.trade_size
            margin = p[ids,None]-candidates[None,:] if side=='BUY' else candidates[None,:]-p[ids,None]
            scores = intensity(c,t,p[ids,None],candidates[None,:],side)*(margin-z[:,None])
            choices = np.argmax(scores,axis=1)
            positive = scores[np.arange(len(ids)),choices]>0
            result[ids[positive]] = candidates[choices[positive]]
        return result

    def save(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        # File handle prevents numpy from silently appending .npz to temporary names.
        temporary = path.with_suffix(path.suffix + '.tmp')
        with temporary.open('wb') as handle:
            np.savez_compressed(handle, config=json.dumps(asdict(self.config)), values=self.values,
                                iterations=np.array(self.iterations))
        temporary.replace(path)

    @classmethod
    def load(cls, path):
        with np.load(path, allow_pickle=False) as data:
            c = ModelConfig(**json.loads(str(data['config'])))
            table = cls(c, data['values'].copy(), data['iterations'].tolist())
        expected = (c.time_steps + 1, c.price_steps + 1, len(table.inventories))
        if table.values.shape != expected or not np.all(np.isfinite(table.values)):
            raise ValueError("Invalid HJB artifact")
        return table


def simulate(table, paths=1000, steps=1000, seed=7, benchmark=False):
    """Same bounded inventory for both policies. Synthetic model validation only."""
    if paths < 1 or steps < 2:
        raise ValueError("Invalid simulation size")
    fill_rng, price_rng, settlement_rng = (np.random.default_rng(s) for s in np.random.SeedSequence(seed).spawn(3))
    c = table.config
    if not c.q_min <= 0 <= c.q_max:
        raise ValueError("Simulation needs zero initial inventory")
    p, q, cash = np.full(paths, .5), np.zeros(paths), np.zeros(paths)
    quotes = np.linspace(c.p_min, 1-c.p_min, c.quote_steps+1)
    myopic = HJBTable(replace(c, gamma=0, gamma_terminal=0), np.zeros_like(table.values))
    policy = myopic if benchmark else table
    for n in range(steps):
        t, dt = n / steps, 1 / steps
        for side in ('BUY', 'SELL'):
            prices = policy.quote_many(t,p,q,side,quotes)
            active = np.isfinite(prices)
            px = np.where(active,prices,.5)
            lam = intensity(c, t, p, px, side) * active
            count = fill_rng.poisson(lam * dt)
            room = (c.q_max - q if side == 'BUY' else q - c.q_min) / c.trade_size
            size = np.minimum(count, np.floor(room + 1e-9)) * c.trade_size
            q += size if side == 'BUY' else -size
            cash += -px * size if side == 'BUY' else px * size
        # Correct Ito drift in logit space preserves the model's price martingale.
        x = np.log(p / (1-p))
        sigma = c.sigma0 + c.sigma1*t**c.eta + c.sigma2/(1+x*x)
        x += -.5 * sigma*sigma*(1-2*p)*dt + sigma*np.sqrt(dt)*price_rng.standard_normal(paths)
        p = np.clip(1/(1+np.exp(-np.clip(x, -700, 700))), c.p_min, 1-c.p_min)
    pnl = cash + q * (settlement_rng.random(paths) < p)
    losses = -pnl
    var = float(np.quantile(losses, .95))
    return dict(paths=paths, seed=seed, benchmark=benchmark, mean_pnl=float(pnl.mean()),
                std_pnl=float(pnl.std()), mean_abs_terminal_inventory=float(np.abs(q).mean()),
                var_95=var, es_95=float(losses[losses >= var].mean()))

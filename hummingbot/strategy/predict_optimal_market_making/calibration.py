"""Conservative online calibration of a stationary subset of paper parameters.

REST volume counts are a quasi-Poisson approximation. We deliberately do NOT
fit all time-varying paper coefficients from a short single-market sample.
"""
from dataclasses import asdict, replace
import json
import math
from pathlib import Path

import numpy as np

from .model import intensity


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, indent=2, allow_nan=False, default=str), encoding='utf-8')
    temporary.replace(path)


def score(config, rows, start, horizon):
    p = np.array([r['mid'] for r in rows])
    px = np.array([r['price'] for r in rows])
    t = np.clip(np.array([(r['utc']-start)/horizon for r in rows]), 0, 1)
    dt = np.array([r['seconds']/horizon for r in rows])
    y = np.array([r['count'] for r in rows])
    buy = np.array([r['side'] == 'BUY' for r in rows])
    rate = np.where(buy, intensity(config, t, p, px, 'BUY'), intensity(config, t, p, px, 'SELL'))
    mu = np.maximum(rate * dt, 1e-15)
    return float(np.sum(y*np.log(mu)-mu))


def fit(config, books, exposures, start, horizon, min_windows=100, min_fills=20):
    """Chronological 70/30 holdout; caller solves and validates before activation."""
    rows = sorted((r for r in exposures if r['eligible'] and start <= r['utc'] <= start+horizon), key=lambda r:r['utc'])
    report = dict(status='insufficient_data', windows=len(rows), active_version=config.version,
                  method='stationary_quasi_poisson_REST_volume', uncertainty='interval_censored')
    if len(rows) < min_windows:
        return None, report
    cut = int(len(rows)*.7)
    # Split whole observation times; no simultaneous buy/sell leakage across boundary.
    boundary = rows[cut]['utc']
    train = [r for r in rows if r['utc'] < boundary]
    valid = [r for r in rows if r.get('start_utc',r['utc']-r['seconds']) >= boundary]
    if not train or not valid:
        return None, report
    report.update(training_end_utc=boundary, train_fills=sum(r['count'] for r in train),
                  validation_fills=sum(r['count'] for r in valid))
    if report['train_fills'] < min_fills or report['validation_fills'] < max(5, min_fills*.2):
        return None, report
    if any(sum(r['count'] for r in train if r['side']==s) < 3 for s in ('BUY', 'SELL')):
        report['status'] = 'insufficient_two_sided_fills'
        return None, report
    offsets = np.array([abs(r['price']-r['mid']) for r in train])
    if np.ptp(offsets) < .002:
        report['status'] = 'insufficient_spread_variation'
        return None, report
    best, best_score = None, -math.inf
    train_p = np.array([r['mid'] for r in train])
    train_price = np.array([r['price'] for r in train])
    train_t = np.clip(np.array([(r['utc']-start)/horizon for r in train]),0,1)
    train_dt = np.array([r['seconds']/horizon for r in train])
    train_buy = np.array([r['side']=='BUY' for r in train])
    for k in np.linspace(5, 100, 96):
        unit = replace(config, a0=1., a1=0., k0=float(k), k1=float(k))
        rates = np.where(train_buy,intensity(unit,train_t,train_p,train_price,'BUY'),
                         intensity(unit,train_t,train_p,train_price,'SELL'))
        expected = float(np.sum(rates*train_dt))
        if expected <= 0:
            continue
        a = report['train_fills']/expected
        if not np.isfinite(a) or not 1e-6 < a < 1e7:
            continue
        candidate = replace(unit, a0=float(a))
        ll = score(candidate, train, start, horizon)
        if ll > best_score:
            best, best_score = candidate, ll
    if best is None:
        report['status'] = 'fit_failed'
        return None, report
    # Estimate a constant latent volatility using training-period samples only.
    b = sorted((r for r in books if start <= r['utc'] < boundary and 0 < r['mid'] < 1),key=lambda r:r['utc'])
    squared, elapsed, count = 0., 0., 0
    for a, z in zip(b, b[1:]):
        dt = z['mono']-a['mono']
        if a.get('session_id') != z.get('session_id') or not 1 <= dt <= 30:
            continue
        dx = np.log(z['mid']/(1-z['mid']))-np.log(a['mid']/(1-a['mid']))
        squared += dx*dx
        elapsed += dt/horizon
        count += 1
    volatility_status = 'unchanged_insufficient_samples'
    if count >= 50 and elapsed > 0:
        sigma = float(np.sqrt(squared/elapsed))
        if .01 <= sigma <= 10:
            volatility_candidate = replace(best,sigma0=sigma,sigma1=0.,sigma2=0.)
            validation_books = sorted((r for r in books if boundary <= r['utc'] <= valid[-1]['utc']),key=lambda r:r['utc'])
            old_vol_score,new_vol_score,vol_n = 0.,0.,0
            for a,z in zip(validation_books,validation_books[1:]):
                dt = (z['mono']-a['mono'])/horizon
                if (a.get('session_id') != z.get('session_id') or not 1/horizon <= dt <= 30/horizon
                        or not 0 < a['mid'] < 1 or not 0 < z['mid'] < 1):
                    continue
                x = np.log(a['mid']/(1-a['mid']))
                dx = np.log(z['mid']/(1-z['mid']))-x
                t = max(0,min(1,(a['utc']-start)/horizon))
                for candidate,is_new in ((config,False),(volatility_candidate,True)):
                    s = candidate.sigma0+candidate.sigma1*t**candidate.eta+candidate.sigma2/(1+x*x)
                    variance = max(1e-15,s*s*dt)
                    residual = dx+.5*s*s*(1-2*a['mid'])*dt
                    ll = -.5*(np.log(variance)+residual*residual/variance)
                    if is_new:
                        new_vol_score += ll
                    else:
                        old_vol_score += ll
                vol_n += 1
            if vol_n >= 20 and new_vol_score > old_vol_score+1:
                best = volatility_candidate
                volatility_status = 'validated_constant_latent_volatility'
            else:
                volatility_status = 'unchanged_validation_not_improved'
    old_score, new_score = score(config,valid,start,horizon), score(best,valid,start,horizon)
    report.update(status='rejected_validation', candidate=asdict(best), candidate_version=best.version,
                  old_validation_score=old_score, new_validation_score=new_score,
                  volatility_samples=count, volatility_status=volatility_status, training_start_utc=train[0]['utc'],
                  validation_end_utc=valid[-1]['utc'])
    # Held-out fill likelihood cannot validate volatility: report it separately.
    if new_score <= old_score + 1. or best.k0 in (5., 100.):
        return None, report
    # Reject abrupt coefficient changes: sparse samples must not destabilize live quotes.
    if not .25 <= best.a0/max(config.a0,1e-9) <= 4 or not .5 <= best.sigma0/config.sigma0 <= 2:
        report['status'] = 'rejected_parameter_jump'
        return None, report
    report['status'] = 'candidate_validated'
    return best, report

"""Poder estatístico 5% x 10% com parâmetros MEDIDOS (coortes reais) e volume observado da Onda 1."""
import math

Z = 1.96 + 0.84          # 95% bilateral, 80% de poder
CASAMENTO = 0.95         # conservador; nas coortes o CPF casou ~100% (o próprio exame é faturado)
VOL = {"Tireoide": 789, "Reumatologia": 204}   # pacientes sorteados/mês, prd 14–22/10

# Por janela: p (retorno), media e dp com teto no p99 (tratamento de outlier), rho com consumo dos 180 dias anteriores.
# Tireoide 90/180: EXTRAPOLADA da 60 pela evolução observada na reumatologia (coorte sem 90/180 dias completos).
PAR = {
    "Reumatologia": {30: (0.635, 11738, 38111, 0.504), 60: (0.745, 17147, 55747, 0.551),
                     90: (0.791, 21650, 68222, 0.607), 180: (0.846, 24732, 70019, 0.639)},
    "Tireoide": {30: (0.602, 2249, 8340, 0.367), 60: (0.713, 3550, 10407, 0.372)},
}
r = PAR["Reumatologia"]
t60 = PAR["Tireoide"][60]
for w in (90, 180):
    k_m = r[w][1] / r[60][1]
    k_cv = (r[w][2] / r[w][1]) / (r[60][2] / r[60][1])
    m = t60[1] * k_m
    PAR["Tireoide"][w] = (min(0.95, t60[0] + (r[w][0] - r[60][0])), m, m * (t60[2] / t60[1]) * k_cv, t60[3])


def var_dif(meses, f, w, kind, cuped):
    """Variância do estimador estratificado por linha (pesos = participação no volume)."""
    ntot = sum(VOL.values())
    v = 0.0
    for l, vol in VOL.items():
        p, m, dp, rho = PAR[l][w]
        n = vol * meses * CASAMENTO
        nc, nn = n * f, n * (1 - f)
        s2 = p * (1 - p) if kind == "ret" else dp ** 2 * ((1 - rho ** 2) if cuped else 1)
        v += (vol / ntot) ** 2 * s2 * (1 / nc + 1 / nn)
    return v


def media_onda(w):
    ntot = sum(VOL.values())
    return sum(vol / ntot * PAR[l][w][1] for l, vol in VOL.items())


def mde(meses, f, w, kind, cuped=False):
    e = Z * math.sqrt(var_dif(meses, f, w, kind, cuped))
    return e if kind == "ret" else e / media_onda(w)


def meses_para(efeito, f, w, kind, cuped=False):
    lo, hi = 0.1, 600
    for _ in range(80):
        mid = (lo + hi) / 2
        lo, hi = (lo, mid) if mde(mid, f, w, kind, cuped) <= efeito else (mid, hi)
    return hi


print("Parâmetros por janela (Tireoide 90/180 extrapolada):")
for l in PAR:
    for w in (30, 60, 90, 180):
        p, m, dp, rho = PAR[l][w]
        print(f"  {l:12s} {w:3d}d  retorno {p:.0%}  média(cap) R$ {m:,.0f}  CV {dp/m:.2f}  rho {rho:.2f}")

print("\nMENOR GANHO DETECTÁVEL — Onda 1 (993 sorteados/mês), por meses de acúmulo")
for w in (30, 60, 90, 180):
    print(f"\n janela {w} dias")
    print("  meses | retorno 5% | retorno 10% | consumo 5% | consumo 10% | consumo+ajuste 5% | consumo+ajuste 10%")
    for H in (3, 6, 9, 12, 18, 24):
        print(f"  {H:5d} | {mde(H,.05,w,'ret')*100:7.1f} pp | {mde(H,.10,w,'ret')*100:8.1f} pp | "
              f"{mde(H,.05,w,'val'):8.0%} | {mde(H,.10,w,'val'):9.0%} | {mde(H,.05,w,'val',True):14.0%} | {mde(H,.10,w,'val',True):15.0%}")

print("\nMESES DE ACÚMULO para enxergar o efeito — janela 90 dias")
for nome, ef, kind, cup in [("retorno +3 pp", .03, "ret", False), ("retorno +5 pp", .05, "ret", False),
                             ("retorno +8 pp", .08, "ret", False), ("consumo +20%", .20, "val", False),
                             ("consumo +20% c/ ajuste", .20, "val", True), ("consumo +30%", .30, "val", False),
                             ("consumo +30% c/ ajuste", .30, "val", True)]:
    a, b = meses_para(ef, .05, 90, kind, cup), meses_para(ef, .10, 90, kind, cup)
    print(f"  {nome:24s}  5%: {a:5.1f}   10%: {b:5.1f}   razão {a/b:.2f}")

"""Reproduz o sorteio descrito na doc e testa as propriedades que o estudo AFIRMA.

Formula declarada em docs/features/randomizacao.md:
  sorteio(cpf) = pmod(conv(substring(sha2(concat(cpf, salt),256),1,15),16,10), 10000)
  grupo        = controle se sorteio < round(fracao * 10000)
"""
import hashlib, random
from collections import Counter

SALT = "rededor-navegacao-v1"

def sorteio(cpf: str, salt: str = SALT) -> int:
    h = hashlib.sha256((cpf + salt).encode()).hexdigest()
    return int(h[:15], 16) % 10000

def dv(base: str) -> str:
    def digito(nums, pesos):
        r = sum(int(n) * p for n, p in zip(nums, pesos)) % 11
        return "0" if r < 2 else str(11 - r)
    d1 = digito(base, range(10, 1, -1))
    d2 = digito(base + d1, range(11, 1, -1))
    return base + d1 + d2

random.seed(42)
N = 1_000_000
cpfs = [dv(f"{random.randrange(10**9):09d}") for _ in range(N)]
buckets = [sorteio(c) for c in cpfs]

ctrl5 = {c for c, b in zip(cpfs, buckets) if b < 500}
ctrl10 = {c for c, b in zip(cpfs, buckets) if b < 1000}

print(f"amostra ................... {N:,} CPFs validos")
print(f"fracao com corte 5% ....... {len(ctrl5)/N*100:.4f}%  (alvo 5,0000%)")
print(f"fracao com corte 10% ...... {len(ctrl10)/N*100:.4f}%  (alvo 10,0000%)")
print()
print("AFIRMACAO 'subir de 5% para 10% NAO realoca ninguem':")
print(f"  os do controle 5% seguem no de 10%? {ctrl5 <= ctrl10}")
print(f"  entram a mais: {len(ctrl10 - ctrl5):,}")
print()
print("AFIRMACAO 'determinismo — a mesma pessoa cai sempre no mesmo braco':")
print(f"  sorteio repetido bate? {all(sorteio(c)==b for c,b in list(zip(cpfs,buckets))[:1000])}")
print()
print("AFIRMACAO R7 'independente de qualquer atributo' — por digito REGIONAL (9o digito):")
por_regiao = Counter()
tot_regiao = Counter()
for c, b in zip(cpfs, buckets):
    r = c[8]
    tot_regiao[r] += 1
    if b < 500:
        por_regiao[r] += 1
pior = 0.0
for r in sorted(tot_regiao):
    n = tot_regiao[r]; k = por_regiao[r]; p = k/n*100
    se = (0.05*0.95/n)**0.5 * 100
    z = (p-5.0)/se
    pior = max(pior, abs(z))
    print(f"  regiao {r}: {k:6,}/{n:7,} = {p:.3f}%   z={z:+.2f}")
print(f"  maior |z| entre as 10 regioes: {pior:.2f}")

print()
print("=" * 74)
print("REVERSIBILIDADE — a pergunta que decide entre comecar com 5% ou com 10%")
print("=" * 74)
sobe = ctrl10 - ctrl5          # 5% -> 10%: quem ENTRA no controle
desce = ctrl10 - ctrl5         # 10% -> 5%: exatamente os mesmos, mas SAINDO
print(f"  5% -> 10%: entram no controle ........ {len(sobe):,} ({len(sobe)/N*100:.2f}% da base)")
print(f"             saem do controle ........... {len(ctrl5 - ctrl10):,}")
print(f"  10% -> 5%: saem do controle .......... {len(desce):,} ({len(desce)/N*100:.2f}% da base)")
print(f"             entram no controle ......... {len(ctrl5 - ctrl5):,}")
print()
print("  Leitura: o corte e MONOTONO. Subir so ACRESCENTA; descer so REMOVE.")
print("  Tecnicamente as duas direcoes sao um numero no WHERE. O que nao e simetrico")
print("  e o EFEITO sobre quem ja foi exposto:")
print(f"    - subindo: os {len(ctrl5):,} do controle inicial seguem intactos, e {len(sobe):,}")
print("      entram DEPOIS (janela de observacao menor -- estudo escalonado).")
print(f"    - descendo: {len(desce):,} pessoas que ja passaram por um periodo SEM navegacao")
print("      voltam a ser navegadas. Viram crossover nao planejado, e o desfecho delas")
print("      deixa de ser atribuivel a um braco so.")
print()
print("  ATENCAO: nao ha impedimento TECNICO de voltar para 5%. Ha custo METODOLOGICO, e ele")
print("     e irreversivel: a nao-navegacao ja aconteceu e nao se desfaz mudando o flag.")

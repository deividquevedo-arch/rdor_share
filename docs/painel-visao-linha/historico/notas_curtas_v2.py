"""Notas curtas de leitura (desvio, outlier, lacuna) por linha x mes, sobre o consolidado v2.

Nada e construido: os numeros sao os consultados; a nota so explica. Gera notas-curtas-v2.json e
acrescenta `nota_curta` a cada registro de mensal-consolidado-linha-v2.json (arquivo novo: -notas).
"""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent
TODOS = [f"2026-{i:02d}" for i in range(1, 11)]

NOTAS = {  # (linha, mes ou '*') -> nota
    ("*", "envio_ate_out"): "Enviados: registro só a partir de 23/10 (plataforma); antes, contagem do próprio job legado.",
    ("ateromatose", "*antes"): "Linha iniciada em jun/26.",
    ("ateromatose", "2026-06"): "Estreia em 24/06 (mês parcial).",
    ("ateromatose", "2026-08"): "Relevantes do legado caem de jun a ago (1.285 → 132); causa não registrada.",
    ("ateromatose", "2026-09"): "Último mês do algoritmo legado.",
    ("ateromatose", "2026-10"): "Troca para a plataforma (régua migrada).",
    ("cancer_colon", "2026-01"): "Linha iniciada em fev/26.",
    ("cancer_colon", "2026-02"): "Estreia em 20/02 (só TC); colonoscopia entra em 30/03.",
    ("cancer_colon", "2026-10"): "Troca para a plataforma: escopo passa a colonoscopia.",
    ("cancer_estomago", "*antes"): "Linha iniciada na plataforma em ago/26.",
    ("cancer_estomago", "2026-07"): "Só exames do fim de jul processados na estreia (parcial).",
    ("cancer_rim", "*antes"): "Linha iniciada na plataforma em set/26.",
    ("cancer_rim", "2026-08"): "Só exames de ago processados na estreia (parcial).",
    ("transplante_pulmao", "*antes"): "Linha iniciada na plataforma em ago/26.",
    ("transplante_pulmao", "2026-07"): "Só exames do fim de jul processados na estreia (parcial).",
    ("tumor_osseo", "*antes"): "Linha iniciada na plataforma em set/26.",
    ("tumor_osseo", "2026-08"): "Só exames do fim de ago processados na estreia (parcial).",
    ("dii", "2026-01"): "Régua anterior: 5,9% de relevantes (0,6% a partir de mar).",
    ("dii", "2026-02"): "Régua anterior: 3,6% de relevantes; colonoscopia entra em 17/03.",
    ("dii", "2026-10"): "Troca para a plataforma (régua migrada).",
    ("doencas_biliares", "*antes"): "Linha iniciada em abr/26.",
    ("doencas_biliares", "*legado"): "Contagem do próprio job (pode repetir exame); sem detalhe por hospital.",
    ("doencas_biliares", "2026-04"): "Mês de estreia (piloto).",
    ("doencas_biliares", "2026-09"): "Só exames de set processados na estreia da plataforma (parcial).",
    ("doencas_biliares", "2026-10"): "Plataforma; mês completo pelo processamento inicial.",
    ("neuroimunologia", "*antes"): "Linha iniciada em abr/26.",
    ("neuroimunologia", "2026-03"): "Só exames do fim de mar processados na estreia (parcial).",
    ("neuroimunologia", "2026-10"): "Troca para a plataforma (régua migrada).",
    ("pirads", "*enave"): "Captação manual no PACS (enfermeira navegadora); algoritmo a partir de mai/26.",
    ("pirads", "2026-04"): "Só exames do fim de abr processados na estreia (parcial).",
    ("pirads", "2026-10"): "Troca para a plataforma: filtro de entrada mais amplo (volume ~3x).",
    ("reumatologia", "2026-01"): "Volume e hospitais crescem até abr/26 (51 → 74 hospitais).",
    ("reumatologia", "2026-02"): "Volume e hospitais crescem até abr/26 (51 → 74 hospitais).",
    ("reumatologia", "2026-03"): "Volume e hospitais crescem até abr/26 (51 → 74 hospitais).",
    ("reumatologia", "2026-08"): "Queda de volume: região craniana retirada do escopo em 23/07.",
    ("reumatologia", "2026-09"): "Troca para a plataforma (régua migrada).",
    ("tirads", "*retro"): "Exames processados em 15/06, na estreia do algoritmo; sem envio no mês.",
    ("tirads", "2026-08"): "Último mês do algoritmo legado.",
    ("tirads", "2026-09"): "Troca para a plataforma: filtro de entrada mais amplo (volume ~3,5x).",
    ("nefrologia", "*"): "Relevante = clearance < 30 (critério do job).",
    ("mama", "*"): "Relevante = BI-RADS ≥ 4; o envio do legado inclui todas as categorias.",
    ("pulmao_nodulo", "*"): "Relevante = indicação de encaminhamento do algoritmo.",
    ("hepatologia", "*"): "Algoritmo legado (a plataforma rodou em paralelo, sem entrega).",
}


def nota(r: dict) -> str | None:
    l, m, f, n = r["linha"], r["mes"], r["fonte"], r.get("nota") or ""
    if f is None:
        return NOTAS.get((l, m)) or NOTAS.get((l, "*antes"))
    for k in ((l, m), (l, "*enave") if f == "legado_pacs_enave" else None,
              (l, "*retro") if "retroativamente" in n and l == "tirads" else None,
              (l, "*legado") if f == "monitoramento_job_legado" else None, (l, "*")):
        if k and k in NOTAS:
            return NOTAS[k]
    return None


if __name__ == "__main__":
    d = json.loads((OUT / "mensal-consolidado-linha-v2.json").read_text(encoding="utf-8"))
    for r in d["linhas"]:
        r["nota_curta"] = nota(r)
    d["meta"]["nota_geral"] = NOTAS[("*", "envio_ate_out")]
    (OUT / "mensal-consolidado-linha-v2-notas.json").write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    for r in d["linhas"]:
        if r["nota_curta"]:
            print(f"{r['linha']:19} {r['mes']}  {r['nota_curta']}")

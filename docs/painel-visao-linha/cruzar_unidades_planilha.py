"""Cruza a extração por unidade com a aba "Unidades IAs" da planilha de portfólio (status de navegação por hospital e linha).

Uso: python cruzar_unidades_planilha.py <visao-por-linha-AAAA-MM-DD.json> <unidades_ias.json> <unidades_gold.json>
     -> cruzamento-unidades-<fim>.json

- Hospital da planilha -> ids de cadastro: primeiro os ids da navegação (mesmo hospital), senão o nome na gold
  (normalizado), senão os casos manuais abaixo.
- Coluna da planilha -> linha do algoritmo (e recorte, para Ca de Cólon e DII).
Só agregados: nenhum dado de paciente.
"""
import json
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

AQUI = Path(__file__).parent
sys.path.insert(0, str(AQUI))
from gerar_painel_visao_por_linha import hospitais_navegados  # noqa: E402

COLUNAS = {  # coluna da planilha -> (linha, recorte) do algoritmo em produção; None = sem algoritmo na plataforma
    "Reumatologia": ("reumatologia", ""), "DII (TC e RM )": ("doenca_inflamatoria_intestinal", "outros exames"),
    "DII (Colonoscopia)": ("doenca_inflamatoria_intestinal", "colonoscopia"),
    "Ca de Colon (TC e RM)": ("cancer_colon", "outros exames"), "Ca de Colon (Colonoscopia)": ("cancer_colon", "colonoscopia"),
    "Doenças Biliares": ("doencas_biliares", ""), "Neuroimunologia": ("neuroimunologia", ""), "Próstata": ("pirads", ""),
    "Ateromatose Coronariana": ("ateromatose_coronariana", ""), "Tireóide": ("tirads", ""),
    "Ca de Estomago (Endoscopias)": ("cancer_estomago", ""), "Transplante de Pulmão": ("transplante_pulmao", ""),
    "Ca de Rim": ("cancer_rim", ""), "TU Osseo": ("tumor_osseo", ""),
}
MANUAL = {  # nome na planilha -> nome do cadastro na gold, quando o nome difere
    "MATERNIDADE SAO LUIZ STAR": "MATERNIDADE STAR",
    "HOSPITAL NOSSA SENHORA DAS NEVES - JARDIM BOTÂNICO": "HOSPITAL NOSSA SENHORA DAS NEVES",
    "HOSPITAL NOSSA SENHORA DAS NEVES - EPITÁCIO": "HNSN EPITACIO",
}


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().upper()
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\b(HOSPITAL|E MATERNIDADE|MATERNIDADE|DE CLINICAS|CLINICAS|DE|DA|DO)\b", " ", s)
    return " ".join(s.split())


def ids_por_hospital(planilha: list[tuple[str, str]], gold: list[dict]) -> dict[str, list[str]]:
    nav = {norm(h[5:]): ids for h, ids in hospitais_navegados().items()}
    por_nome = defaultdict(list)
    for g in gold:
        por_nome[norm(g["nme_hospital"])].append(g["id_unidade"])
    out = {}
    for _, nome in planilha:
        k = norm(MANUAL.get(nome, nome))
        out[nome] = nav.get(k) or por_nome.get(k) or []
    return out


def main(arq_dados: str, arq_unid: str, arq_gold: str) -> None:
    dados = json.loads(Path(arq_dados).read_text(encoding="utf-8"))
    v = json.loads(Path(arq_unid).read_text(encoding="utf-8"))
    cab = v[3]
    linhas_planilha = [r for r in v[4:] if r[4]]
    planilha = [(r[3], r[4]) for r in linhas_planilha]
    ids = ids_por_hospital(planilha, json.loads(Path(arq_gold).read_text(encoding="utf-8")))
    por_unid = defaultdict(lambda: {"exames": 0, "relevantes": 0, "enviados": 0})
    for r in dados["linhas"]:
        a = por_unid[(r["linha"], r["variante"], r["unidade"])]
        for c in ("exames", "relevantes", "enviados"):
            a[c] += int(r[c])
    hospitais = []
    for r in linhas_planilha:
        uf, nome = r[3], r[4]
        for col, chave in COLUNAS.items():
            status = (r[cab.index(col)] or "").strip()
            soma = {"exames": 0, "relevantes": 0, "enviados": 0}
            for u in ids[nome]:
                for c in soma:
                    soma[c] += por_unid.get((*chave, u), {}).get(c, 0)
            hospitais.append({"coluna": col, "linha": chave[0], "recorte": chave[1], "uf": uf, "hospital": nome,
                              "status_planilha": status, "cadastro_no_repositorio": bool(ids[nome]), **soma})
    out = AQUI / f"cruzamento-unidades-{dados['fim']}.json"
    out.write_text(json.dumps({"inicio": dados["inicio"], "fim": dados["fim"], "controle_envio": dados["controle_envio"],
                               "sem_cadastro": [n for n, i in ids.items() if not i], "hospitais": hospitais},
                              ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, "|", len(planilha), "hospitais |", len(hospitais), "linhas | sem cadastro:", [n for n, i in ids.items() if not i])
    for col in COLUNAS:
        hs = [h for h in hospitais if h["coluna"] == col]
        nav = [h for h in hs if h["status_planilha"] == "Navegado"]
        print(f"{col:32} navegados {len(nav):3}  com exame {sum(h['exames'] > 0 for h in nav):3}  "
              f"com exame e nao navegado {sum(h['exames'] > 0 and h['status_planilha'] != 'Navegado' for h in hs):3}  "
              f"exames {sum(h['exames'] for h in hs):7}")


if __name__ == "__main__":
    main(*sys.argv[1:4])

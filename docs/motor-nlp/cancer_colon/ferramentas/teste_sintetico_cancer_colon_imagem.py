"""Teste sintético da config `cancer_colon_imagem` contra a nlp-engine-lib (sem PHI: frases escritas à mão).

Uso:
    PYTHONPATH=<src da lib na tag v0.12.3> python teste_sintetico_cancer_colon_imagem.py <config.py>

Sai com código 1 se algum caso divergir do esperado. Casos marcados `conhecido` documentam a diferença de motor
registrada na SPEC (desempate "órgão mais próximo vence" do legado, sem equivalente na lib): não reprovam, mas o
resultado é impresso para acompanhar.
"""
import sys

from nlp_engine.nlp_engine.config_loader import load as carregar_config
from nlp_engine.nlp_engine.engine import ClinicalNlpEngine

LONGE = " ".join(["Demais estruturas abdominais sem alterações significativas ao método."] * 5)

CASOS = [
    ("lesao_afirmada", "TC DE ABDOME TOTAL. Cólon sigmoide com lesão vegetante estenosante.", 1, None),
    ("tumor_massa_reto", "Observa-se massa no reto distal, com densificação da gordura adjacente.", 1, None),
    ("polipo_ceco", "Imagem compatível com pólipo séssil no ceco.", 1, None),
    ("ulceracao_colon", "Cólon transverso com área de ulceração mucosa.", 1, None),
    ("neg_sem", "Cólon descendente sem lesões.", 0, None),
    ("neg_nao_ha", "Não há sinais de neoplasia no cólon.", 0, None),
    ("neg_ausencia", "Ausência de ulceração no cólon transverso.", 0, None),
    ("sem_colon", "Rins com cistos simples. Fígado de dimensões normais.", 0, None),
    ("so_ancora", "Diverticulose colônica não complicada.", 0, None),
    ("achado_longe_do_colon", "Cólon de calibre normal. " + LONGE + " Lesão hepática hipodensa no segmento VII.", 0, None),
    ("outro_orgao_mesma_frase", "Lesão hepática hipodensa no segmento VII; alças colônicas de calibre normal.", 0,
     "conhecido"),
]


def main(cfg_path: str) -> int:
    ns: dict = {}
    exec(compile(open(cfg_path, encoding="utf-8").read(), cfg_path, "exec"), ns)
    # Mesmo caminho do runner: a config passa pelo `load()` da lib, que valida e normaliza `findings` por entidade
    # (`terms`/`regex`/`label`) para a forma interna. Chamar `process()` com o dict cru não carrega os achados.
    cfg = carregar_config(ns["CONFIG"])
    linhas =[{"id_exame": nome, "exm_laudo_texto": texto, "exm_mod": "TC", "exm_tipo": "TC DE ABDOME",
               "dt_exame": "2026-09-01"} for nome, texto, _, _ in CASOS]
    saida = ClinicalNlpEngine(engine_version="sintetico").process(
        linhas, cfg["nlp"], specialty_id=cfg["specialty_id"], config_version=cfg["config_version"])
    falhas = 0
    for (nome, _, esperado, marca), out in zip(CASOS, saida, strict=True):
        obtido = int(out["fl_relevante"])
        ok = obtido == esperado
        if not ok and marca != "conhecido":
            falhas += 1
        rotulo = "ok" if ok else ("CONHECIDO" if marca == "conhecido" else "FALHA")
        print(f"{rotulo:9} {nome:26} esperado={esperado} obtido={obtido} achados={out.get('findings')!r}")
    print(f"\n{len(CASOS)} casos, {falhas} falha(s)")
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))

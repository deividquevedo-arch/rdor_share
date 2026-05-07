Fechar S02 por AC: completar T02.2 para ficar mais fiel ao legado (Matcher spaCy + regex accent-tolerant de achados + filtro de proximidade órgão↔achado), não só o núcleo atual em rule_engine.py.
Decidir T02.4 com o time: validar onde fica quality_guard (em monitoring ou no próprio nlp_engine) e registrar a decisão no board antes de codar.
Aumentar testes de engine: adicionar cenários objetivos em tests/test_rule_engine.py para proximidade, ambiguidade de órgão, negação multi-token e regressão por especialidade.
Congelar contrato de config: sair de {especialidade}_config.yaml genérico para configs reais por especialidade (camada shared/organs.yaml + specialty), alinhado ao doc-contrato-runtime.
Preparar S03 em paralelo: evoluir config_loader.py com validação/merge de schema YAML; hoje está mínimo e é o próximo gargalo de governança.
Rodar validação local com dados reais: usar _local_samples (gold/diamond) como smoke de completude (schema + taxa de match), com critério explícito de aceite no board.
Depois disso: integrar no notebook/composition root para E2E controlado e só então abrir para expansão semântica (semantic_expand.py, fase posterior).


Fechar S02 com foco no motor: completar T02.2 no nlp_engine (Matcher spaCy, regex accent-tolerant, proximidade) e consolidar T02.3 em cima disso.
Limitar T02.4 no nlp_engine: manter só invariantes locais (payload parseável, ranges básicos) e remover expectativa de quality_guard pesado dentro da lib.
Formalizar decisão de arquitetura: registrar no board/nota que métricas agregadas e validação de qualidade operacional ficam na lib monitoring.
Definir contrato de telemetria do motor: quais sinais por registro o nlp_engine sempre entrega para o monitoring (ex.: n_positive_spans, n_negated_spans, confidence_score, versão de config/engine).
Iniciar stream monitoring: implementar cálculo batch de métricas (precision/recall/F1 quando houver rótulo, taxa de positivos, distribuição de score, drift).
Avançar S03 em paralelo: config_loader com schema/merge (shared/organs.yaml + specialty) para garantir governança e evitar hardcode.
Gate de evolução: manter ruff + pytest verde no nlp_engine e adicionar um smoke local com _local_samples só para regressão rápida (sem virar critério único de aceite clínico).
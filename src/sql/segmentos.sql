        SELECT T1.*,            -- Realizar nova Subquery de todo código dentro do FROM
                CASE WHEN pct_receita <= 0.5 AND pct_freq <= 0.5 THEN 'BAIXO v. BAIXO F.'       -- Realizando comparações para pegar os seller_ids que tiveram receita e frequencia menor que 50%
                        WHEN pct_receita > 0.5 AND pct_freq <= 0.5 THEN 'ALTO VALOR'            -- Receita maior que 50% E frequência menor ou igual a 50%
                        WHEN pct_receita <= 0.5 AND pct_freq > 0.5 THEN 'ALTA FREQ'             -- Receita menor ou igual a 50% E frequencia maior que 50%
                        WHEN pct_receita < 0.9 OR pct_freq < 0.9 THEN 'PRODUTIVO'               -- Receita menor que 90% OU frequencia menor que 90%
                        ELSE 'SUPER PRODUTIVO'          -- Se não estiver atrelado a nenhuma das comparações anteriores será 'SUPER PRODUTIVO' (Valor E Frequencia acima de 90%)
                END AS SEGMENTO_VALOR_FREQ,              -- Finaliza as comparações e coloca o nome da coluna como 'SEGMENTO_VALOR_FREQ'

                CASE WHEN qtde_dias_base <= 60 THEN 'INICIO'            -- Realizando comparação para identificar situação do seller_id
                        WHEN qtde_diAS_ult_venda >= 300 THEN 'RETENCAO'         -- Dias da última venda maios que 300 dias
                        ELSE 'ATIVO'            -- Se não for nenhuma das comparações anteriores então será 'ATIVO'
                END AS SEGMENTO_VIDA,           -- Finalizar as comparações e coloca o nome da coluna como SEGMENTO_VIDA
                '{date_end}' AS DT_SGMT         -- Colocando a data da "FOTO(momento)" desta tabela

        FROM(
                SELECT T1.*,            -- Realizando SubQuery de todo código dentro do FROM
                        percent_rank() OVER(ORDER BY receita_total ) as pct_receita,         -- Pegar percentual da receita_total dentro do FROM e apelido pct_receita
                        percent_rank() OVER(ORDER BY qtde_pedidos asc) as pct_freq              -- Pegar percentual da qtde_pedidos dentro do FROM e apelido pct_freq

                FROM(

                        SELECT T2.seller_id,        -- Pegando seller_id da tabela tb_order_items
                                SUM(T2.price) AS receita_total,     -- Soma total de todos os price da tabela tb_order_items
                                COUNT(DISTINCT T1.order_id) AS qtde_pedidos,        -- Contar todos os pedidos da tabela tb_orders
                                COUNT(T2.product_id) AS qtde_produtos,      -- Contar todos os produtos da tabela tb_order_items
                                COUNT(DISTINCT T2.product_id) AS qtde_distinct_produtos,        --Contar todos os produtos diferentes da tabela tb_order_items
                                MIN(CAST(julianday('{date_end}') - julianday(T1.order_approved_at) AS INT ) ) AS qtde_diAS_ult_venda,        -- Total de diAS desde a última venda, conta realizada pela subtração dAS datAS através do calendário Juliano
                                MAX(CAST(julianday('{date_end}') - julianday(T3.dt_inicio) AS INT) ) AS qtde_dias_base        --Total de dias que o seller_id está cadastrado

                        FROM tb_orders AS T1        --Buscando na tabela tb_orders e coloca apelidto T1
                        LEFT JOIN tb_order_items AS T2      -- Buscando na tabela tb_order_items e coloca apelido T2
                        ON T1.order_id = T2.order_id        -- Comparando order_ID dAS tabelAS T1 e T2

                        LEFT JOIN (             
                                SELECT T2.seller_id, MIN(DATE(T1.order_approved_at)) AS dt_inicio      -- Novo JOIN para filtrar a data inicial de cada seller_id
                                FROM tb_orders AS T1            -- Buscando na tabela tb_orders e coloca apelido T1
                                LEFT JOIN tb_order_items AS T2          -- Buscando outra tabela tb_order_items e coloca apelido T2 
                                ON T1.order_id = T2.order_id            -- Comparando os order_id para trazer a informação equivalente
                                GROUP BY T2.seller_id                   -- Agrupando por seller_id
                                ) AS T3 ON T2.seller_id = T3.seller_id          -- Comparando os seller_id para trazer a informação equivalente


                        WHERE T1.order_approved_at BETWEEN  '{date_init}' AND '{date_end}'       --  Filtrando a busca com uma data entre '{date_init}' e '{date_end}' dentro da tabela T1 na coluna order_approved_at
                        GROUP BY T2.seller_id       -- Agrupando toda a busca através dos seller_id

                        /* Este select agrupa e mostra todos os vendedores(seller_id) e a partir disso mostra a receita total do seller_id pegando a soma
                        ** de todo o 'price' daquele seller_id, mostra a contagem de cada pedido realizado pelo seller_id, a contagem de produtos que cada
                        ** seller_id solicitou, mostra a contagem de produtos diferentes que cada seller_id solicitou e mostra a diferença em diAS
                        ** desde a última venda aprovada pelo cliente através do calendário Juliano (julianday) e transformando em número inteiro através do
                        ** CAST AS INT*/

                ) AS T1         -- Apelido SubQuery 1

        ) AS T1
WHERE seller_id IS NOT NULL
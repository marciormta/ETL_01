from etl import pipeline_vendas
#pode escolher entre csv ou parquet.
pasta = 'data'
formato_saida = ['csv']
pipeline_vendas(pasta, formato_saida)

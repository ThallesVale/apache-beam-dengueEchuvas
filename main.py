import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options = pipeline_options)

colunas_arquivo = ['id','data_iniSE','casos','ibge_code','cidade','uf','cep','latitude','longitude']

def lista_para_dicionario(elemento,colunas):
    '''
        Receberá duas listas e retorna um dicionário
    '''
    return dict(zip(colunas,elemento))


def texto_para_lista(elemento,delimitador ='|'):
    '''
        Receberá um elemento txt, delimitado por um separador. Default, para este caso,pipe ('|').
        Retornará uma lista de strings, separados, considerando delimitador inputado
    '''
    return elemento.split(delimitador)

def trata_data(elemento):
    '''
        Receberá um dicionário e cria um novo campo de data composto por ANO-MES(YYYY-MM)
        Retornará o mesmo dicionário com o novo campo acrescido 
    '''
    delimiter = '-'
    elemento['ano_mes'] = delimiter.join(elemento['data_iniSE'].split('-')[:2])
    
    return elemento

def chave_uf(elemento):
    '''
        Receberá um dicionário e retornará uma tupla composta por (UF, dicionario)
    '''
    
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    '''
        Recebe tupla com dados agrupados pelo UF.
        Retornará tupla da junção de UF+Data. Ex.: (MG-2021-12, [{},{},...,{n}])
    '''
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d',registro['casos'])):
           yield (uf+'-'+registro['ano_mes'],float(registro['casos']))
        else:
            yield (uf+'-'+registro['ano_mes'],0.0)


def chave_uf_ano_mes(elemento):
    """
        Receberá um lista e retornará uma tupla
        com valor de estado-ano-mes e volume de chuva. EX.: ('UF-2021-12',12.5)
    """
    data,mm,uf = elemento
    delimiter = '-'
    ano_mes = delimiter.join(data.split('-')[:2])

    chave = f'{uf}-{ano_mes}'

    if float(mm)<0:
        mm = 0.0
    else:
        mm = float(mm)

    return chave, mm

def arredonda(elemento):
    '''
        Recebe tupla, retorna tupla arredondada
    '''
    chave,mm = elemento
    return (chave, round(mm,1))

def filtra_campos_vazios(elemento):
    '''
        Remove linhas que apresentem campos vazios 
        Recebe uma tupla e retorna as tuplas que apresentem informação completa
    '''
    chave, dados = elemento
    if all([dados['chuvas'],dados['casos_dengue']]):
        return True
    return False
def descompactar_elementos(elemento):
    """
        Recebe uma tupla composta por chave(id) e um dicionário com os valores de chuva e casos de dengue
        Retorna tupla com valores de id, chuva e casos de dengue descompactados
    """

    chave,dados = elemento

    uf, ano, mes = chave.split('-')
    chuvas = str(dados['chuvas'][0])
    dengue = str(int(dados['casos_dengue'][0]))

    return uf, ano, mes, chuvas, dengue

def preparar_csv(elemento,delimitador = ';'):
    """
        Recebe uma tupla com dados descompactados
        Retorna dados estruturados em arquivo csv
    """
    
    return delimitador.join(elemento)

#pcollections
#   | -> passos

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText('sample_casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario,colunas_arquivo)
    | "Criar campo ano_mes" >> beam.Map(trata_data)
    | "Criar chave pelo UF" >> beam.Map(chave_uf)
    | "Agrupar pelo UF" >> beam.GroupByKey()
    | "Manipula e cria nova tupla" >> beam.FlatMap(casos_dengue)
    | "Soma casos pela chave" >> beam.CombinePerKey(sum)
    #| "Mostrar resultados" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> ReadFromText('sample_chuvas.csv', skip_header_lines=1)
    | "De texto para lista (chuvas)" >> beam.Map(texto_para_lista, delimitador=',')
    | "Criar chave UF_ano_mes" >> beam.Map(chave_uf_ano_mes)
    | "Soma quantidade chuva pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar resultados" >> beam.Map(arredonda)
    #| "Mostrar resultados das chuvas" >> beam.Map(print)
)

resultado = (
    ({'chuvas':chuvas, 'casos_dengue': dengue})
    | "Mesclar pcollections (merge)" >> beam.CoGroupByKey()
    | "Filtrar dados vazios" >> beam.Filter(filtra_campos_vazios)
    | "Descompactar elementos da tupla" >> beam.Map(descompactar_elementos)
    | "Organizar dados no formato de arquivo csv" >> beam.Map(preparar_csv)
    #| "Mostrar resultados da união de pcollections" >> beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;CASOSDENGUE'

resultado | "Persistir arquivo CSV" >> WriteToText('resultado',file_name_suffix ='.csv' ,header = header )

pipeline.run()

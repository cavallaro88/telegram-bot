from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
import mariadb
import nest_asyncio
nest_asyncio.apply()
import logging
from telegram import constants,Update
from telegram.ext import Application,CommandHandler,ConversationHandler,ContextTypes,MessageHandler, filters
from unidecode import unidecode

#import schedule

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

ADD_PRODUTOS, DEL_PRODUTOS = range(2)

def busca_olx(produtos):
    #Parametros
    # produtos=['xps 13','controle ps4', 'agenda']
    qte_paginas=2
    lista_itens=[]
    
    for tupla in produtos:
        for produto in tupla:
            #URL
            prefixo = "https://sp.olx.com.br/?o="
            sufixo="&q="
            termo_buscado=produto.replace(" ", "%20")
            
            #contador
            i=1
            
            #laço pelas páginas
            while i <= qte_paginas:  
                # Request da página e soup
                headers = { 'Accept-Language' : "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                            'User-Agent':"requests.get(url=your_url,headers=headers)"}
                pagina = requests.get(url=prefixo+str(i)+sufixo+termo_buscado,headers=headers)
                pagina_html = pagina.text
                pagina_soup = BeautifulSoup(pagina_html, 'html.parser')
                
                # Busca do nome, local, preço e link do anúncio
                lista_termo=pagina_soup.find_all(
                    'h2',{'data-ds-component':"DS-Text"})
                local_termo=pagina_soup.find_all(
                    'span',{'data-ds-component':"DS-Text",
                            'aria-label': re.compile("Localização:.*")})
                preco_termo=pagina_soup.find_all(
                    'span',{'data-ds-component':"DS-Text",
                            'aria-label': re.compile("Preço do item:.*")})
                link_termo=pagina_soup.find_all(
                    'a',{'data-ds-component':"DS-Link",
                         'data-lurker-detail':"list_id"},href=True)
                # Cria lista de tuplas
                for item,local,preco,link in zip(lista_termo,local_termo,preco_termo,link_termo):
                    lista_itens.append(
                        (produto,item.text,local.text,preco.text.replace("R$ ","").replace(".",""),link["href"]))
        
                i+=1
                 
        #Criação dataframe    
        df_itens_olx=pd.DataFrame(lista_itens, columns=['termo_buscado','nome','local','preco','link'])
        #Substituir valores em branco para NaN
        df_itens_olx=df_itens_olx.mask(df_itens_olx =='')
        #Alterar preço para float
        df_itens_olx['preco']= df_itens_olx['preco'].astype('float')
        #Retirar itens NaN
        df_itens_olx = df_itens_olx.dropna()
        
        df_itens_olx['termo_existente']=df_itens_olx.apply(lambda row:
                                     row['termo_buscado'].lower() in row['nome'].lower(),axis=1)
        
        #Levar dados para o banco de dados na tabela de anuncios
        for index, row in df_itens_olx.iterrows():
            cur.execute("""
                               INSERT INTO anuncios (
                                   termo_buscado,
                                   link,
                                   nome,
                                   local,
                                   preco,
                                   termo_existente)
                               VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE
                                   termo_buscado = VALUE(termo_buscado),
                                   link = VALUE(link),
                                   nome = VALUE(nome),
                                   local = VALUE(local),
                                   preco = VALUE(preco),
                                   termo_existente = VALUE(termo_existente)
                                   """,
                               (row['termo_buscado'],
                               row['link'],
                               row['nome'],
                               row['local'],
                               row['preco'],
                               row['termo_existente']))
            #Tornar as mudanças persistentes        
            con.commit()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Usuário %s abriu a lista de comandos.", update.message.from_user.first_name)
    await update.message.reply_text(
        '''
        Posso te ajudar a encontrar o melhor preço no OLX SP\!
        \- */start* \- Mensagem de boas vindas
        \- */add\_prod* \- Insiro produtos na sua lista de monitoramento
        \- */del\_prod* \- Removo produtos na sua lista de monitoramento
        \- */busca* \- Apresento os anúncios mais baratos da sua lista de monitoramento
        ''',parse_mode=constants.ParseMode.MARKDOWN_V2)
        
async def ecoar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Usuário %s escreveu %s.", update.message.from_user.first_name,update.message.text)
    nome_tupla = (update.message.from_user.first_name, update.message.from_user.last_name)
    nome_completo = ' '.join(nome_tupla)
    cur.execute("INSERT INTO mensagens (id,texto_enviado) VALUES (?,?) ON DUPLICATE KEY UPDATE id = VALUE(id), texto_enviado = VALUE(texto_enviado)",\
                (update.message.from_user.id,update.message.text))
    cur.execute("INSERT INTO usuarios (id,username, nome, idioma) VALUES (?,?,?,?) \
                ON DUPLICATE KEY UPDATE id = VALUE(id), username = VALUE(username), nome = VALUE(nome), idioma = VALUE(idioma)",\
                (update.message.from_user.id,update.message.from_user.username, nome_completo, update.message.from_user.language_code))
    con.commit()
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Você escreveu: "+update.message.text)

async def msg_add_prod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    #oportunidade de transformar o input do usuário em uma função evitando código duplicado!
    nome_tupla = (update.message.from_user.first_name, update.message.from_user.last_name)
    nome_completo = ' '.join(nome_tupla)
    cur.execute("INSERT INTO usuarios (id,username, nome, idioma) VALUES (?,?,?,?) \
                 ON DUPLICATE KEY UPDATE id = VALUE(id), username = VALUE(username), nome = VALUE(nome), idioma = VALUE(idioma)",\
                 (update.message.from_user.id,update.message.from_user.username, nome_completo, update.message.from_user.language_code))

    #Bot sugere como os itens devem ser adicionados
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Ok! Escreva abaixo os produtos que deseja monitorar separando-os por virgula.\nEx: iphone 10, tesoura")

    return ADD_PRODUTOS

async def msg_del_prod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    #Bot sugere como os itens devem ser adicionados
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Ok! Escreva abaixo os produtos que NÃO deseja monitorar separando-os por virgula.\nEx: iphone 10, tesoura")

    return DEL_PRODUTOS

async def add_prod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    #Extração e tratamento dos termos após comando /add_prod:
    produtos_usuario = update.message.text.split(',')
    unidecode_produtos = [unidecode(x).strip().lower() for x in produtos_usuario if unidecode(x).strip()!= '']
    
    # Envio da lista para o database
    for x in unidecode_produtos:
        cur.execute("INSERT INTO anuncio_usuario (id, termo_buscado) VALUES (?,?) ON DUPLICATE KEY UPDATE id = VALUES(id), termo_buscado = VALUES(termo_buscado)",\
                    (update.message.from_user.id,x))        
        con.commit()
    
    # Lista dos itens ativos 
    cur.execute("SELECT termo_buscado FROM anuncio_usuario WHERE id = ? ",(update.message.from_user.id,))
    produtos_usuario = ', '.join([str(x) for t in cur.fetchall() for x in t])
    
    await update.message.reply_text("Lista de produtos ativos: "+str(produtos_usuario))

    return ConversationHandler.END

async def del_prod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    #Extração e tratamento dos termos após comando /del_prod:
    produtos_usuario = update.message.text.split(',')
    unidecode_produtos = [unidecode(x).strip().lower() for x in produtos_usuario if unidecode(x).strip()!= '']
    
    # Apagar produtos ativos do database
    for x in unidecode_produtos:
        cur.execute("DELETE FROM anuncio_usuario WHERE id = ? AND termo_buscado = ?",\
                    (update.message.from_user.id,x))       
        con.commit()
    
    # Lista dos itens ativos 
    cur.execute("SELECT termo_buscado FROM anuncio_usuario WHERE id = ?",(update.message.from_user.id,))
    produtos_usuario = ', '.join([str(x) for t in cur.fetchall() for x in t])

    await update.message.reply_text("Lista de produtos ativos: "+str(produtos_usuario))
    
    return ConversationHandler.END

async def run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur.execute("SELECT termo_buscado FROM status_anuncios WHERE id = ? AND termos_distintos IS NULL",(update.message.from_user.id,))
    produtos_usuario = cur.fetchall()
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Processando...")
  
    #Web scrapping OLX da lista que não há histórico
    busca_olx(produtos_usuario)
    
    #Retorno da lista de anuncios mais baratos
    cur.execute("""
            SELECT 
            termo_buscado, link, nome, local, preco 
            FROM ranking_anuncio A
            WHERE A.termo_buscado in (SELECT termo_buscado from anuncio_usuario WHERE id = ?) 
            """,(update.message.from_user.id,))
    anuncios = cur.fetchall()
    msg=''
    for anuncio in anuncios:
        msg_lista = ['R$'+str(x) if isinstance(x, float) else x for x in anuncio]
        msg = msg + f'<b>{msg_lista[2]}</b> - <i>{msg_lista[4]}</i>\n{msg_lista[3]}\n{msg_lista[1]}'
        msg = msg +'\n\n'

    await context.bot.send_message(chat_id=update.effective_chat.id, text=msg,parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)    

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    logger.info("Usuário %s cancelou a conversa p/ add_prod.", update.message.from_user.first_name)
    await update.message.reply_text(
        "O comando foi cancelado. Posso fazer algo por você?\nMande /start para vera lista de comandos.")

    return ConversationHandler.END

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Desculpe não entendo esse comando")

# def executar(update: Update, context: CallbackContext):
#     termos_set = set(termos) #itens distintos
#     df = busca_olx(list(termos_set))
#     buf = io.BytesIO()
#     df.to_excel(buf)
#     buf.seek(0) 
#     #print(buf)
#     buf.name = "ofertas.xlsx"
#     chat_id = update.message.chat_id
#     context.bot.send_document(chat_id, buf)

if __name__ == '__main__':
    #Criação do cursor para interagir com o banco de dados
    con = mariadb.connect(
        user="USERNAME",
        password="PASSWORD",
        host="IP",
        port=3306,
        database="BATABASE"

    )
    cur = con.cursor()

    application = Application.builder().token('5657814891:AAE1mE2-WMfl8_gvdmhYcufd7W0tvrbeSeI').build()
    
    start_handler = CommandHandler('start', start)
    run_handler = CommandHandler('busca', run)
    conv_add_prod_handler = ConversationHandler(
        entry_points=[CommandHandler('add_prod', msg_add_prod)],
        states={
            ADD_PRODUTOS: [MessageHandler(filters.Regex("^[a-zA-Z\u00E0-\u00FF\d, ]*$"), add_prod)] #Letras com e sem acento, números e virgula
        },
        fallbacks=[MessageHandler(filters.Regex("^[^a-zA-Z\u00E0-\u00FF\d, ]*$"),cancel)], #Exatamente o oposto do acima
    )
    conv_del_prod_handler = ConversationHandler(
        entry_points=[CommandHandler('del_prod', msg_del_prod)],
        states={
            DEL_PRODUTOS: [MessageHandler(filters.Regex("^[a-zA-Z\u00E0-\u00FF\d, ]*$"), del_prod)] #Letras com e sem acento, números e virgula
        },
        fallbacks=[MessageHandler(filters.Regex("^[^a-zA-Z\u00E0-\u00FF\d, ]*$"),cancel)], #Exatamente o oposto do acima
    )
    echo_handler = MessageHandler(filters.TEXT & (~filters.COMMAND), ecoar)
    unknown_handler = MessageHandler(filters.COMMAND, unknown)

    application.add_handler(start_handler)
    application.add_handler(conv_add_prod_handler)
    application.add_handler(conv_del_prod_handler)
    application.add_handler(echo_handler)
    application.add_handler(run_handler)
    application.add_handler(unknown_handler)
    
    application.run_polling()



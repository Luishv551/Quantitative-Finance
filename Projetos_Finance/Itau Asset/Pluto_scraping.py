from distutils.log import error
from gc import get_stats
from xml.dom.minidom import Element
from xml.sax.xmlreader import Locator
import pyperclip
import pandas
import numpy as np
import time
import pickle
import pyautogui as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


email = 'email'
senha = 'password'
tickets = ['ABCB2', 'AALR3', 'ABCB4', 'ABRE11', 'ABRE3', 'AEDU3', 'AERI3', 'AESB3', 'AGRO3', 'ALLD3', 'ALLL3', 'ALPA4', 'ALSC3', 'ALSO3', 'ALUP11', 'ALUP12', 'AMAR3', 'AMBP3', 'ANIM3', 'ARML3', 'ARTR3', 'ARZZ3', 'AUTM3', 'AZUL4', 'BBRK3', 'BEEF3', 'BIDI11', 'BIDI4', 'BISA3', 'BKBR3', 'BLAU3', 'BMGB4', 'BMOB3', 'BOAS3', 'BPAN4', 'BPHA1', 'BPHA3', 'BRAP4', 'BRIN3', 'BRML3', 'BRPR3', 'BRSR6', 'BTOW3', 'CAML3', 'CARD3', 'CASH3', 'CBAV3', 'CCXC3', 'CEAB3', 'CESP6', 'CGAS11', 'CGAS5', 'CIEL3', 'CLSA3', 'CNTO3', 'COGN3', 'CPLE6', 'CSED3', 'CSMG1', 'CSMG3', 'CSNA3', 'CTIP1', 'CTIP3', 'CURY3', 'CVCB3', 'CYRE3', 'DASA3', 'DEXP3', 'DIRR3', 'DMMO3', 'DTEX3', 'DXCO3', 'ECOR3', 'ELPL3', 'ELPL4', 'EMBR3', 'ENAT3', 'ENBR1', 'ENBR3', 'ENEV1', 'ENEV3', 'ENJU3', 'EQTL3', 'ESPA3', 'ESTC3', 'EVEN3', 'EZTC3', 'FESA4', 'FJTA4', 'FLRY3', 'GETI3', 'GETI4', 'GETT11', 'GFSA1', 'GFSA11', 'GFSA12', 'GFSA3', 'GGPS3', 'GMAT3', 'GOAU4', 'GOLL12', 'GOLL2', 'GOLL4', 'GRND3', 'GUAR3', 'HBOR3', 'HBSA3', 'HGTX3', 'HRTP3', 'IFCM3', 'IGTA3', 'IGTI11', 'INTB3', 'IRBR3', 'JALL3', 'JHSF3', 'JPSA3', 'JSLG3',
           'KEPL3', 'LAVV3', 'LCAM3', 'LEVE3', 'LIGT3', 'LINX3', 'LJQQ3', 'LLIS3', 'LLXL3', 'LOGG3', 'LOGN3', 'LPSB3', 'LWSA3', 'MAGG3', 'MATD3', 'MDIA3', 'MEAL3', 'MEGA3', 'MGLU3', 'MILS3', 'MLAS3', 'MMXM3', 'MODL11', 'MOVI3', 'MPLU3', 'MRFG3', 'MRVE3', 'MTRE3', 'MULT3', 'MYPK3', 'NGRD3', 'ODPV3', 'OGXP3', 'OIBR3', 'OIBR4', 'OMGE3', 'ONCO3', 'OPCT3', 'ORVR3', 'OSXB1', 'OSXB3', 'PARC3', 'PARD3', 'PCAR3', 'PDGR3', 'PETZ3', 'PFRM1', 'PFRM3', 'PGMN3', 'PLPL3', 'PMAM3', 'PNVL3', 'POMO4', 'POSI3', 'PRIO3', 'PRML3', 'PTBL3', 'QGEP3', 'QUAL3', 'RAIZ4', 'RANI3', 'RAPT10', 'RAPT4', 'RCSL3', 'RCSL4', 'RECV3', 'RLOG3', 'ROMI3', 'RRRP3', 'RSID3', 'RUMO3', 'SAPR11', 'SAPR4', 'SBFG3', 'SEER3', 'SEQL3', 'SIMH3', 'SLCE3', 'SLED4', 'SMLE3', 'SMLS3', 'SMTO3', 'SOJA3', 'SOMA3', 'SQIA3', 'SSBR3', 'STBP11', 'STBP3', 'SULA11', 'SYNE3', 'TAEE11', 'TASA4', 'TCSA3', 'TECN3', 'TEND3', 'TESA3', 'TGMA3', 'TIET11', 'TOTS3', 'TPIS3', 'TRAD3', 'TRIS3', 'TRPL4', 'TTEN3', 'TUPY3', 'UNIP6', 'USIM12', 'USIM5', 'VIIA3', 'VIVA3', 'VIVR3', 'VLID3', 'VULC3', 'VVAR11', 'VVAR3', 'WIZS3', 'XBM1', 'XBM2', 'YDUQ3']
# tickets = ['MODL11', 'OIBR4', 'ROMI3']


def copyTable():
    pyperclip.copy('')
    time.sleep(2)
    pd.moveTo(648, 679)
    pd.scroll(-600)
    pd.moveTo(1190, 224)
    pd.click()
    pd.moveTo(254, 296)
    pd.click()
    pd.click()
    pd.click()
    time.sleep(1)
    pd.keyDown('ctrl')
    pd.keyDown('c')
    pd.keyUp('c')
    pd.keyUp('ctrl')

    cola = pyperclip.paste()
    s = cola.split("\r\n")
    s = pandas.DataFrame({'DF': s})
    s.replace('', np.nan, inplace=True)
    s.dropna(inplace=True)

    if(s.shape[0] > 20):
        s = s['DF'].str.split("\t", expand=True)
        nomes_colunas = s.iloc[0][1:]
        s = s.iloc[1:, :]
        n = s.shape[0]
        indices = list(range(1, n, 2))
        indices_ind = list(range(0, n, 2))
        selecao = s.iloc[indices_ind]
        indicadores = selecao.iloc[:, 0]
        aux = s.iloc[indices]
        aux = aux.iloc[:, :-1]
        aux = aux.fillna(0)
        aux.index = indicadores
        aux.index.name = None
        aux.columns = nomes_colunas
        aux.columns.name = None
        return aux

    else:
        print('invalida')
        pass


def click(x, y):
    pd.moveTo(x, y)
    pd.click()


def findElement(driver, element_xpath, timeout=20):
    element = None

    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, element_xpath)))
    except:
        print(f"Xpath não encontado: {element_xpath}")

    return element


driver = webdriver.Chrome(
    'C:\\Users\\User\\Desktop\\chromedriver_win32\\chromedriver.exe')
action = ActionChains(driver)
driver.get("https://investidor10.com.br/")
driver.maximize_window()


# Digitando a acao no campo de busca
xpath_busca = '/html/body/div[3]/section[1]/div/div[1]/div/form/div/span/input[2]'
busca = findElement(driver, xpath_busca)
busca.send_keys('LOGG3')
busca.submit()

# Pressionando no botão de resultado da busca
xpath_small_cap = '//*[@id="results"]/div/div[2]/div[1]/div'
btn_small_cap = findElement(driver, xpath_small_cap)
btn_small_cap.click()

# abrindo a aba de login
xpath_ten = '//*[@id="quotation-section"]/header/div/ul/li[5]'
btn_ten = findElement(driver, xpath_ten)
btn_ten.click()

# Logando no site
xpath_email = '//*[@id="modal-sign"]/div/div[1]/form/div[1]/input'
xpath_password = '//*[@id="modal-sign"]/div/div[1]/form/div[2]/input'
submit_email = findElement(driver, xpath_email).send_keys(email)
submit_password = findElement(driver, xpath_password).send_keys(senha)
xpath_log = '//*[@id="modal-sign"]/div/div[1]/form/div[3]/input'
log = findElement(driver, xpath_log)
log.click()

time.sleep(2)
elemento = '//*[@id="indicators-history"]/header/div/ul/li[3]'
btn_elemento = findElement(driver, elemento)
try:
    ActionChains(driver).move_to_element(btn_elemento).perform()
except:
    print()
lista_tabelas = {}
lista_tabelas['LOGG3'] = copyTable()

for acao in tickets:
    time.sleep(1)
    # reseta site
    menu_xpath = '/html/body/div[2]/header/div[1]/div/div[1]/a'
    btn_menu = findElement(driver, menu_xpath)
    btn_menu.click()
    time.sleep(2)

    click(627, 419)
    pd.write(acao)
    pd.keyDown('enter')
    pd.keyUp('enter')

    btn_small_cap = findElement(driver, xpath_small_cap)
    try:
        # pressiona botão
        btn_small_cap.click()

    except:
        print('ticket fora do site')
        continue

    time.sleep(2)
    elemento = '//*[@id="indicators-history"]/header/div/ul/li[3]'
    btn_elemento = findElement(driver, elemento)

    try:
        ActionChains(driver).move_to_element(btn_elemento).perform()
    except:
        print()
    lista_tabelas[acao] = copyTable()

with open('arquivo_exportado2.pkl', 'wb') as arquivo:
    pickle.dump(lista_tabelas, arquivo)

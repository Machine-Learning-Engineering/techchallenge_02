import time
import logging
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import os
import sys
from typing import List, Dict

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ibov_scraper.log')
    ]
)
logger = logging.getLogger(__name__)


class IBOVScraper:
    """Scraper para dados do IBOV da B3 com navegação completa por páginas."""
    
    def __init__(self, headless: bool = True, timeout: int = 30):
        """
        Inicializa o scraper.
        
        Args:
            headless: Se deve executar o browser em modo headless
            timeout: Timeout para operações do Selenium
        """
        self.url = os.getenv("IBOV_URL", "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br")
        self.timeout = timeout
        self.headless = headless
        self.driver = None
        self.all_data = []
        
        # Criar diretório data se não existir
        os.makedirs('data', exist_ok=True)
        
        # Configurar driver
        self.driver = self._setup_driver()
    
    def _setup_driver(self) -> webdriver.Chrome:
        """Configura o driver do Chrome."""
        try:
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless")
            
            # Configurações para estabilidade
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.set_page_load_timeout(self.timeout)
            
            return driver
            
        except WebDriverException as e:
            logger.error(f"Erro ao configurar driver: {e}")
            raise
    
    def _wait_for_table_load(self):
        """Aguarda o carregamento completo da tabela."""
        try:
            # Aguardar pela presença da tabela
            wait = WebDriverWait(self.driver, self.timeout)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            
            # Aguardar um pouco mais para garantir carregamento dos dados JavaScript
            time.sleep(3)
            
            # Verificar se há linhas de dados na tabela
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            if not rows:
                # Tentar seletor alternativo
                rows = self.driver.find_elements(By.CSS_SELECTOR, "table tr")
            
            logger.info(f"Tabela carregada com {len(rows)} linhas")
            return len(rows) > 0
            
        except TimeoutException:
            logger.error("Timeout ao aguardar carregamento da tabela")
            return False
    
    def _extract_table_data(self, page_number: int) -> List[Dict]:
        """
        Extrai dados da tabela da página atual.
        
        Args:
            page_number: Número da página atual
            
        Returns:
            Lista de dicionários com os dados das ações
        """
        try:
            data = []
            
            # Tentar diferentes seletores para encontrar a tabela
            table_selectors = [
                "table",
                "table.table",
                "[role='table']",
                ".table-responsive table"
            ]
            
            table = None
            for selector in table_selectors:
                try:
                    table = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if table:
                        break
                except NoSuchElementException:
                    continue
            
            if not table:
                logger.error("Nenhuma tabela encontrada na página")
                return data
            
            # Extrair linhas da tabela
            rows = table.find_elements(By.TAG_NAME, "tr")
            logger.info(f"Encontradas {len(rows)} linhas na tabela")
            
            # Identificar linha de cabeçalho (pode ser a primeira)
            header_found = False
            
            for i, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    # Se não há células td, pode ser cabeçalho (th)
                    if not cells:
                        cells = row.find_elements(By.TAG_NAME, "th")
                        if cells and not header_found:
                            header_found = True
                            logger.info(f"Cabeçalho identificado na linha {i}")
                            continue
                    
                    # Verificar se temos dados suficientes
                    if len(cells) >= 5:
                        # Extrair texto das células
                        cell_texts = [cell.get_attribute('textContent').strip() for cell in cells]
                        
                        # Filtrar linhas vazias ou inválidas
                        codigo = cell_texts[0] if len(cell_texts) > 0 else ""
                        empresa = cell_texts[1] if len(cell_texts) > 1 else ""
                        tipo = cell_texts[2] if len(cell_texts) > 2 else ""
                        quantidade = cell_texts[3] if len(cell_texts) > 3 else ""
                        participacao = cell_texts[4] if len(cell_texts) > 4 else ""
                        
                        # Validar se não é linha vazia ou de separação
                        if codigo and empresa and codigo != "Código" and codigo != "":
                            data.append({
                                'codigo': codigo,
                                'empresa': empresa,
                                'tipo': tipo,
                                'quantidade_teorica': quantidade,
                                'participacao_pct': participacao,
                                'pagina': page_number,
                                'data_coleta': datetime.now().isoformat(),
                                'url_fonte': self.driver.current_url
                            })
                            
                except Exception as e:
                    logger.debug(f"Erro ao processar linha {i}: {e}")
                    continue
            
            logger.info(f"Página {page_number}: {len(data)} registros extraídos")
            return data
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados da tabela: {e}")
            return []
    
    def _find_next_page_button(self) -> bool:
        """
        Encontra e clica no botão de próxima página.
        
        Returns:
            True se encontrou e clicou, False caso contrário
        """
        try:
            # Lista de seletores para botões de próxima página
            next_selectors = [
                "a[aria-label='Next']",
                "a[title='Next']",
                "a[title='Próxima']",
                "button[aria-label='Next']",
                "button[title='Next']",
                "button[title='Próxima']",
                "a:contains('Next')",
                "a:contains('Próxima')",
                "a:contains('>')",
                ".pagination a[aria-label='Next']",
                ".pagination button[aria-label='Next']",
                ".pagination a:last-child",
                "a.page-link[aria-label='Next']",
                "li.page-item:last-child a",
                "a[data-dt-idx]",  # DataTables
                "button.paginate_button.next",
                ".dataTables_paginate .next"
            ]
            
            for selector in next_selectors:
                try:
                    # Buscar elemento
                    if ":contains(" in selector:
                        # XPath para texto
                        xpath_selector = f"//a[contains(text(), '{selector.split(':contains(')[1].split(')')[0]}')]"
                        elements = self.driver.find_elements(By.XPATH, xpath_selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            # Verificar se não está desabilitado
                            disabled = element.get_attribute('disabled')
                            aria_disabled = element.get_attribute('aria-disabled')
                            class_attr = element.get_attribute('class') or ""
                            
                            if not disabled and aria_disabled != 'true' and 'disabled' not in class_attr.lower():
                                logger.info(f"Tentando clicar no botão 'próxima' usando seletor: {selector}")
                                
                                # Scroll para o elemento
                                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                                time.sleep(1)
                                
                                # Tentar clicar
                                try:
                                    element.click()
                                except:
                                    # Usar JavaScript como alternativa
                                    self.driver.execute_script("arguments[0].click();", element)
                                
                                logger.info("Clique realizado com sucesso")
                                return True
                                
                except Exception as e:
                    logger.debug(f"Seletor {selector} falhou: {e}")
                    continue
            
            # Tentar buscar por números de página
            try:
                # Buscar links de páginas numéricas
                page_links = self.driver.find_elements(By.XPATH, "//a[matches(text(), '^[0-9]+$')]")
                
                if page_links:
                    # Pegar o maior número
                    current_page_text = "1"
                    try:
                        current_indicators = self.driver.find_elements(By.CSS_SELECTOR, ".active, .current, [aria-current='page']")
                        if current_indicators:
                            current_page_text = current_indicators[0].get_attribute('textContent').strip()
                    except:
                        pass
                    
                    try:
                        current_page_num = int(current_page_text)
                        next_page_num = current_page_num + 1;
                        
                        # Buscar link da próxima página
                        next_page_link = self.driver.find_element(By.XPATH, f"//a[text()='{next_page_num}']")
                        if next_page_link.is_displayed() and next_page_link.is_enabled():
                            logger.info(f"Navegando para página {next_page_num}")
                            next_page_link.click()
                            return True
                            
                    except (ValueError, NoSuchElementException):
                        pass
            
            except Exception as e:
                logger.debug(f"Erro ao buscar páginas numéricas: {e}")
            
            logger.info("Nenhum botão de próxima página encontrado")
            return False
            
        except Exception as e:
            logger.error(f"Erro ao buscar botão de próxima página: {e}")
            return False
    
    def _check_if_last_page(self) -> bool:
        """
        Verifica se estamos na última página.
        
        Returns:
            True se for a última página, False caso contrário
        """
        try:
            # Verificar se botão Next está desabilitado
            disabled_selectors = [
                "a[aria-label='Next'][aria-disabled='true']",
                "button[aria-label='Next'][disabled]",
                "a[title='Next'].disabled",
                "button[title='Next']:disabled",
                ".pagination .next.disabled",
                ".dataTables_paginate .next.disabled"
            ]
            
            for selector in disabled_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    logger.info(f"Última página detectada via seletor: {selector}")
                    return True
            
            # Verificar se há texto indicando última página
            page_text = self.driver.page_source.lower()
            last_page_indicators = [
                "última página",
                "last page",
                "fim da lista",
                "end of list"
            ]
            
            for indicator in last_page_indicators:
                if indicator in page_text:
                    logger.info(f"Última página detectada via texto: {indicator}")
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Erro ao verificar última página: {e}")
            return False
    
    def _set_items_per_page(self, items: int = 120) -> bool:
        """
        Configura o número de itens por página usando o seletor.
        Tenta diferentes valores: 120, 60, 40 em ordem decrescente.
        
        Args:
            items: Número de itens por página preferido
            
        Returns:
            True se configurou com sucesso, False caso contrário
        """
        try:
            logger.info(f"Tentando configurar {items} itens por página")
            
            # Aguardar o carregamento do seletor
            wait = WebDriverWait(self.driver, self.timeout)
            select_element = wait.until(
                EC.element_to_be_clickable((By.ID, "selectPage"))
            )
            
            # Criar objeto Select
            select = Select(select_element)
            
            # Listar opções disponíveis
            options = [option.get_attribute('value') for option in select.options]
            logger.info(f"Opções disponíveis no seletor: {options}")
            
            # Tentar valores em ordem decrescente
            valores_tentativa = [str(items), "60", "40", "20"]
            
            for valor in valores_tentativa:
                if valor in options:
                    try:
                        # Tentar primeiro por valor
                        select.select_by_value(valor)
                        logger.info(f"✅ Configurado para {valor} itens por página (por valor)")
                        
                        # Aguardar o recarregamento da página
                        time.sleep(5)
                        
                        return True
                        
                    except Exception as e1:
                        logger.debug(f"Erro ao selecionar {valor} por valor: {e1}")
                        try:
                            # Tentar por texto visível
                            select.select_by_visible_text(valor)
                            logger.info(f"✅ Configurado para {valor} itens por página (por texto)")
                            
                            # Aguardar o recarregamento da página
                            time.sleep(5)
                            
                            return True
                            
                        except Exception as e2:
                            logger.debug(f"Erro ao selecionar {valor} por texto: {e2}")
                            continue
            
            logger.warning("Não foi possível configurar nenhum valor maior que o padrão")
            return False
            
        except Exception as e:
            logger.error(f"Erro ao configurar itens por página: {e}")
            return False

    def scrape_all_pages(self, max_pages: int = 50) -> List[Dict]:
        """
        Faz scraping de todas as páginas disponíveis.
        Primeiro tenta configurar 120 itens por página para reduzir paginação.
        
        Args:
            max_pages: Número máximo de páginas para processar (proteção)
            
        Returns:
            Lista com todos os dados coletados
        """
        self.all_data = []
        current_page = 1
        
        try:
            logger.info(f"Iniciando scraping do IBOV - máximo {max_pages} páginas")
            logger.info(f"URL: {self.url}")
            
            # Acessar primeira página
            self.driver.get(self.url)
            
            # Aguardar carregamento inicial
            time.sleep(5)
            
            # Tentar configurar o maior número possível de itens por página
            max_items_configured = False
            if self._set_items_per_page(120):
                max_items_configured = True
                logger.info("✅ Configurado para máximo de itens por página - coletando tudo otimizado!")
            else:
                logger.warning("⚠️ Usando configuração padrão de paginação")
            
            while current_page <= max_pages:
                logger.info(f"=== Processando página {current_page} ===")
                
                # Aguardar carregamento da tabela
                if not self._wait_for_table_load():
                    logger.error(f"Falha ao carregar tabela na página {current_page}")
                    break
                
                # Extrair dados da página atual
                page_data = self._extract_table_data(current_page)
                
                if page_data:
                    self.all_data.extend(page_data)
                    logger.info(f"Página {current_page}: {len(page_data)} registros coletados")
                    
                    # Se coletamos muitos registros (40+ em uma página), 
                    # provavelmente conseguimos configurar para mostrar mais itens
                    if len(page_data) >= 40:
                        logger.info("🎉 Grande quantidade de dados coletados - configuração otimizada funcionou!")
                        logger.info("Verificando se existem mais páginas...")
                    elif len(page_data) >= 60:
                        logger.info("🚀 Excelente! Muitos dados coletados de uma vez!")
                else:
                    logger.warning(f"Página {current_page}: Nenhum dado encontrado")
                
                # Verificar se é a última página
                if self._check_if_last_page():
                    logger.info(f"Última página detectada: {current_page}")
                    break
                
                # Tentar navegar para próxima página (caso ainda haja paginação)
                if current_page < max_pages:
                    if self._find_next_page_button():
                        # Aguardar carregamento da nova página
                        time.sleep(3)
                        current_page += 1
                    else:
                        logger.info("Não foi possível encontrar próxima página - todos os dados podem ter sido coletados")
                        break
                else:
                    logger.info(f"Limite máximo de páginas atingido: {max_pages}")
                    break
            
            logger.info(f"Scraping concluído! Total: {len(self.all_data)} registros de {current_page} página(s)")
            return self.all_data
            
        except Exception as e:
            logger.error(f"Erro durante scraping: {e}")
            return self.all_data
    
    def save_to_csv(self, filename: str = None) -> str:
        """
        Salva todos os dados coletados em um único arquivo CSV.
        
        Args:
            filename: Nome do arquivo (opcional)
            
        Returns:
            Caminho do arquivo salvo
        """
        if not self.all_data:
            logger.warning("Nenhum dado para salvar")
            return ""
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ibov_complete_data_{timestamp}.csv"
        
        # Garantir que está no diretório data
        if not filename.startswith('data/'):
            filename = f"data/{filename}"
        
        try:
            df = pd.DataFrame(self.all_data)
            
            # Ordenar por página e código
            df = df.sort_values(['pagina', 'codigo'])
            
            # Remover duplicatas
            df = df.drop_duplicates(subset=['codigo', 'pagina'], keep='first')
            
            # Salvar CSV
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            logger.info(f"Dados salvos em: {filename}")
            logger.info(f"Total de registros únicos: {len(df)}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Erro ao salvar CSV: {e}")
            return ""
    
    def get_summary(self) -> Dict:
        """Retorna resumo dos dados coletados."""
        if not self.all_data:
            return {}
        
        df = pd.DataFrame(self.all_data)
        
        summary = {
            'total_registros': len(self.all_data),
            'registros_unicos': df.drop_duplicates(subset=['codigo']).shape[0],
            'paginas_processadas': df['pagina'].nunique(),
            'empresas_unicas': df['empresa'].nunique(),
            'primeira_coleta': df['data_coleta'].min(),
            'ultima_coleta': df['data_coleta'].max()
        }
        
        # Estatísticas por página
        summary['registros_por_pagina'] = df.groupby('pagina').size().to_dict()
        
        return summary
    
    def close(self):
        """Fecha o driver."""
        if self.driver:
            self.driver.quit()
            self.driver = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """
    Função principal para executar o scraping completo do IBOV.
    Usa o seletor de 120 itens por página para coletar todos os dados de uma vez.
    """
    print("="*60)
    print("🚀 SCRAPING COMPLETO DO IBOV - MODO OTIMIZADO")
    print("🎯 Configurando para máximo de itens por página")
    print("="*60)
    
    # Opção de debug - mude para False para ver o navegador
    USE_HEADLESS = True
    
    try:
        # Criar scraper
        with IBOVScraper(headless=USE_HEADLESS, timeout=30) as scraper:
            
            # Fazer scraping com configuração otimizada
            print("📊 Iniciando coleta otimizada de dados...")
            print("🔧 Tentando configurar máximo de itens por página...")
            all_data = scraper.scrape_all_pages(max_pages=10)  # Aumentado para capturar mais páginas se necessário
            
            if all_data:
                # Salvar dados em CSV
                print("💾 Salvando dados...")
                csv_file = scraper.save_to_csv()
                
                # Obter resumo
                summary = scraper.get_summary()
                
                # Exibir resultados
                print("\n" + "="*60)
                print("✅ SCRAPING CONCLUÍDO COM SUCESSO!")
                print("="*60)
                print(f"📈 Total de registros coletados: {summary.get('total_registros', 0)}")
                print(f"🏢 Empresas únicas: {summary.get('empresas_unicas', 0)}")
                print(f"📄 Páginas processadas: {summary.get('paginas_processadas', 0)}")
                print(f"🔢 Códigos únicos: {summary.get('registros_unicos', 0)}")
                
                # Verificar se conseguiu otimizar
                total_registros = summary.get('total_registros', 0)
                if total_registros >= 40:
                    print(f"🎉 BOA OTIMIZAÇÃO! Coletados {total_registros} registros")
                    print("✨ Configuração de itens por página funcionou!")
                elif total_registros >= 60:
                    print(f"🚀 EXCELENTE! Coletados {total_registros} registros")
                    print("✨ Máxima otimização alcançada!")
                
                if 'registros_por_pagina' in summary:
                    print(f"\n📊 Registros por página:")
                    for pagina, count in summary['registros_por_pagina'].items():
                        print(f"   Página {pagina}: {count} registros")
                
                if csv_file:
                    print(f"\n💾 Arquivo salvo: {csv_file}")
                
                print("="*60)
                
                # Mostrar algumas amostras dos dados
                if len(all_data) > 0:
                    print("\n📋 Primeiros 5 registros:")
                    df_sample = pd.DataFrame(all_data[:5])
                    print(df_sample[['codigo', 'empresa', 'participacao_pct', 'pagina']].to_string(index=False))
                    
                    if len(all_data) > 5:
                        print(f"\n📋 Últimos 5 registros:")
                        df_sample_last = pd.DataFrame(all_data[-5:])
                        print(df_sample_last[['codigo', 'empresa', 'participacao_pct', 'pagina']].to_string(index=False))
                
            else:
                print("❌ Nenhum dado foi coletado. Verifique os logs para mais detalhes.")
                print("💡 Dica: Mude USE_HEADLESS=False para debug visual.")
                
    except KeyboardInterrupt:
        print("\n⏹️  Scraping interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro na execução principal: {e}")
        print(f"❌ Erro durante execução: {e}")
        print("💡 Dica: Mude USE_HEADLESS=False para debug visual")


if __name__ == "__main__":
    main()

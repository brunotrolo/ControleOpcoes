/**
 * @fileoverview API Client (Oplab & Brapi) - v3.0 (Turbo RAM Cache)
 * Foco: Extrair dados brutos com latência reduzida e resiliência de cota.
 */

const ApiClient = {

  /**
   * Faz o fetch HTTP com Retry Automático e Exponential Backoff.
   * @private
   */
  _fetchData(url, options = {}, retries = 3) {
    const fetchOptions = {
      method: "get",
      muteHttpExceptions: true,
      ...options
    };

    for (let i = 0; i < retries; i++) {
      try {
        const response = UrlFetchApp.fetch(url, fetchOptions);
        const code = response.getResponseCode();
        const content = response.getContentText();
        
        if (code === 200) return JSON.parse(content);
        if (code === 204) return null; 
        
        if (code === 429 || content.includes("quota exceeded")) {
           throw new Error("QUOTA_LIMIT");
        }

        console.warn(`[ApiClient] API HTTP ${code} na URL: ${url}`);
        return null;

      } catch (e) {
        if (i === retries - 1) {
          console.error(`[ApiClient] Falha final após ${retries} tentativas: ${e.message}`);
          return null;
        }
        const waitTime = Math.pow(2, i + 1) * 1000;
        console.warn(`[ApiClient] Erro de rede/cota. Tentativa ${i + 1}/${retries}. Aguardando ${waitTime}ms...`);
        Utilities.sleep(waitTime);
      }
    }
  }
};

// ============================================================================
// SERVIÇO: OPLAB API (Com RAM Cache)
// ============================================================================

const OplabService = {
  _baseUrl: "https://api.oplab.com.br/v3",
  _tokenCache: null, // <--- CACHE EM RAM
  
  _getHeaders() {
    // Se o token já foi lido nesta execução, não chama o PropertiesService
    if (this._tokenCache) return { "Access-Token": this._tokenCache };

    const token = PropertiesService.getScriptProperties().getProperty("OPLAB_ACCESS_TOKEN");
    if (!token) throw new Error("Token OPLAB_ACCESS_TOKEN ausente.");
    
    this._tokenCache = token.trim();
    return { "Access-Token": this._tokenCache };
  },

  /** Interface para Detalhes de OPÇÕES */
  getOptionDetails(ticker) {
    if (!ticker) return null;
    const url = `${this._baseUrl}/market/options/details/${ticker.toUpperCase()}`;
    return ApiClient._fetchData(url, { headers: this._getHeaders() });
  },

  /** Interface para Dados de ATIVOS (Stocks) */
  getStockData(ticker) {
    if (!ticker) return null;
    const url = `${this._baseUrl}/market/stocks/${ticker.toUpperCase()}`;
    return ApiClient._fetchData(url, { headers: this._getHeaders() });
  },

  /** Interface para o Histórico Dados de ATIVOS em 250D (Stocks) */
  getHistoricalData(ticker, amount = 250) {
    if (!ticker) return null;
    const url = `${this._baseUrl}/market/historical/${ticker.toUpperCase()}/1d?amount=${amount}&smooth=true&df=iso`;
    return ApiClient._fetchData(url, { headers: this._getHeaders() });
  },

  /** Interface para buscar todas as opções de um ticker */
  getOptionsByTicker(ticker) {
    if (!ticker) return null;
    const url = `${this._baseUrl}/market/options/${ticker.toUpperCase()}`;
    return ApiClient._fetchData(url, { headers: this._getHeaders() });
  },

  /** Interface para Cálculo Black-Scholes Externo */
  calculateBS(params) {
    if (!params || !params.symbol) return null;
    const query = Object.keys(params)
      .map(k => `${encodeURIComponent(k)}=${encodeURIComponent(params[k])}`)
      .join('&');
    const url = `${this._baseUrl}/market/options/bs?${query}`;
    return ApiClient._fetchData(url, { headers: this._getHeaders() });
  }
};

// ============================================================================
// SERVIÇO: BRAPI API (Com RAM Cache)
// ============================================================================

const BrapiService = {
  _baseUrl: "https://brapi.dev/api/quote",
  _tokenCache: null, // <--- CACHE EM RAM
  
  _getToken() {
    if (this._tokenCache) return this._tokenCache;

    const token = PropertiesService.getScriptProperties().getProperty("BRAPI_ACCESS_TOKEN");
    if (!token) throw new Error("Token BRAPI_ACCESS_TOKEN ausente.");
    
    this._tokenCache = token.trim();
    return this._tokenCache;
  },

  getFundamentalData(ticker) {
    if (!ticker) return null;
    const url = `${this._baseUrl}/${ticker.toUpperCase()}?modules=defaultKeyStatistics,financialData`;
    const res = ApiClient._fetchData(url, {
      headers: { "Authorization": `Bearer ${this._getToken()}` }
    });
    
    if (!res || !res.results || res.results.length === 0) return null;
    
    const data = res.results[0];
    const stats = data.defaultKeyStatistics || {};
    const finData = data.financialData || {};

    const extractRaw = (obj, key) => {
      if (!obj || obj[key] === undefined) return 0;
      return (typeof obj[key] === 'object' && obj[key] !== null) ? Number(obj[key].raw) : Number(obj[key]);
    };

    return {
      lpa: extractRaw(stats, 'trailingEps') || Number(data.earningsPerShare) || 0,
      vpa: extractRaw(stats, 'bookValue'),
      pl: Number(data.priceEarnings) || 0,
      pvp: extractRaw(stats, 'priceToBook'),
      dy: (Number(data.dividendsYield) || 0) / 100, 
      roe: extractRaw(finData, 'returnOnEquity'), 
      divida: 0 
    };
  }
};

// ============================================================================
// SUÍTE DE TESTES E HOMOLOGAÇÃO
// ============================================================================

function testSuiteApiClient() {
  console.log("=== INICIANDO HOMOLOGAÇÃO API CLIENT v3.0 ===");

  // Teste 1: Performance do Cache de Token (OpLab)
  const t0 = Date.now();
  OplabService._getHeaders(); // 1ª leitura (I/O)
  const t1 = Date.now();
  OplabService._getHeaders(); // 2ª leitura (RAM)
  const t2 = Date.now();
  
  console.log(`[PERF] 1ª Leitura Token (Properties): ${t1 - t0}ms`);
  console.log(`[PERF] 2ª Leitura Token (RAM Cache): ${t2 - t1}ms`);
  console.log(`[PERF] Ganho de Velocidade: ${((t1-t0) / (t2-t1 || 1)).toFixed(1)}x`);

  // Teste 2: Conectividade Real OpLab (Ativo)
  console.log("--- Testando Conectividade OpLab (PETR4) ---");
  const stock = OplabService.getStockData("PETR4");
  if (stock && stock.symbol) {
    console.log(`✅ [OpLab] OK: Preço ${stock.symbol} = R$ ${stock.close}`);
  } else {
    console.error("❌ [OpLab] Falha na resposta.");
  }

  // Teste 3: Conectividade Real Brapi (Fundamentos)
  console.log("--- Testando Conectividade Brapi (VALE3) ---");
  const fundamental = BrapiService.getFundamentalData("VALE3");
  if (fundamental && fundamental.lpa !== undefined) {
    console.log(`✅ [Brapi] OK: LPA = ${fundamental.lpa}`);
  } else {
    console.error("❌ [Brapi] Falha na resposta.");
  }

  console.log("=== FIM DA HOMOLOGAÇÃO ===");
}
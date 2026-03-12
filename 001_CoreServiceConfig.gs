/**
 * @fileoverview ConfigManager - v5.0 (Data Dictionary & Clean Architecture)
 * RESPONSABILIDADE: Centralizar o Dicionário Universal de Dados (DUD) e gerenciar o cache.
 * PADRÃO: Nomes de Planilha em UPPER_SNAKE_CASE | Chaves JSON em camelCase.
 */

const SYS_CONFIG = {
  // 1. MAPEAMENTO DE ABAS (Nomes exatos no Google Sheets)
  SHEETS: {
    IMPORT:         "NECTON_IMPORT",
    COCKPIT:        "COCKPIT",
    LOGS:           "LOGS",
    DETAILS:        "DADOS_DETALHES",
    ASSETS:         "DADOS_ATIVOS",
    GREEKS_API:     "DADOS_GREEKS",
    GREEKS_CALC:    "CALC_GREEKS",
    CONFIG:         "CONFIG_GLOBAL",
    HIST_250D:      "DADOS_ATIVOS_HISTORICO250D",
    TREND:          "DADOS_ATIVOS_HISTORICO_TENDENCIA",
    PREDICTIVE:     "PONTUACAO_PREDITIVA_CONSOLIDADA",
    ESTATISTICA:    "ANALISE_ESTATISTICA_ATIVOS",
    FUNDAMENTAL:    "ANALISE_FUNDAMENTALISTA_ATIVOS",
    HEATMAP:        "ANALISE_PREDITIVA_HEATMAP",
    SCANNER:        "SCANNER_OPORTUNIDADES",
    SCANNER_TREND:  "SCANNER_TENDENCIA_OPORTUNIDADES",
    SELECTION_OPT:  "SELECAO_OPCOES",
    SELECTION_STR:  "SELECAO_STRANGLES",
    MACRO:          "DADOS_MACRO_SETORIAL"
  },

  // 2. DICIONÁRIO UNIVERSAL DE DADOS (DUD)
  // Mapeia o [Rótulo da Planilha] -> [Chave JSON para o Web App]
  DUD: {
    "ID_TRADE":       "tradeId",
    "ID_STRATEGY":    "strategyId",
    "TICKER":         "ticker",         // Ação (ex: PETR4)
    "OPTION_TICKER":  "optionTicker",   // Opção (ex: PETRC425)
    "SPOT":           "spot",           // Preço da Ação
    "STRIKE":         "strike",         // Preço de Exercício
    "EXPIRY":         "expiry",         // Vencimento
    "STATUS_OP":      "status",         // Status Operacional
    "SIDE":           "side",           // C ou V
    "QUANTITY":       "quantity",
    "ENTRY_PRICE":    "entryPrice",
    "LAST_PREMIUM":   "lastPremium",
    "UPDATED_AT":     "updatedAt",
    "DELTA":          "delta",
    "GAMMA":          "gamma",
    "THETA":          "theta",
    "VEGA":           "vega",
    "IV":             "iv",
    "IV_RANK":        "ivRank",
    "DTE":            "dte",
    "PL_PCT":         "plPct",
    "PL_VALUE":       "plValue",
    "MONEYNESS":      "moneyness",
    "SCORE":          "score"
  }
};

/**
 * Gerenciador de Configurações com Cache de 3 Camadas
 */
const ConfigManager = {
  _memoryCache: null,
  _cacheKey: "APP_GLOBAL_CONFIGS_V5",
  _cacheTime: 21600, // 6 horas

  /**
   * Obtém configurações dinâmicas da aba CONFIG_GLOBAL.
   */
  get() {
    if (this._memoryCache) return this._memoryCache;

    const cache = CacheService.getScriptCache();
    const cachedData = cache.get(this._cacheKey);
    if (cachedData) {
      this._memoryCache = JSON.parse(cachedData);
      return this._memoryCache;
    }

    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const sheet = ss.getSheetByName(SYS_CONFIG.SHEETS.CONFIG);
      if (!sheet) return {};

      const data = sheet.getDataRange().getValues();
      const configs = {};
      
      for (let i = 1; i < data.length; i++) {
        const key = String(data[i][0]).trim();
        let val = data[i][1];
        if (key && !key.startsWith("//")) {
          // Limpeza de números via DataUtils
          configs[key] = (typeof val === 'string' && val.includes(',')) ? 
                          DataUtils.safeFloat(val) : val;
        }
      }

      this._memoryCache = configs;
      cache.put(this._cacheKey, JSON.stringify(configs), this._cacheTime);
      return configs;
    } catch (e) {
      console.error(`[ConfigManager] Erro no I/O: ${e.message}`);
      return {};
    }
  },

  /**
   * Invalida os caches.
   */
  clearCache() {
    CacheService.getScriptCache().remove(this._cacheKey);
    this._memoryCache = null;
  }
};

function obliterarCacheConfig() {
  // 1. Limpa a RAM temporária
  const cache = CacheService.getScriptCache();
  cache.removeAll(["CONFIG_GLOBAL_CACHE", "CONFIG_CACHE", "SYS_CONFIG"]);
  
  // 2. Limpa o Cofre Profundo
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty("CONFIG_GLOBAL_CACHE");
  props.deleteProperty("CONFIG_CACHE");
  
  console.log("💥 Nuke disparado! Todos os caches foram obliterados.");
}

function CSI_PlanilhaConfig() {
  console.log("=== 🕵️‍♂️ CSI: LENDO PLANILHA CRUA ===");
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Config_Global");
  const data = sheet.getDataRange().getValues();
  
  let encontradas = 0;
  
  for(let i=0; i < data.length; i++) {
    const chave = String(data[i][0]).trim();
    if(chave === "Regra_Qtd_Max_PUT" || chave === "Regra_Qtd_Max_CALL") {
      console.log(`🚨 FLAGRANTE na Linha ${i + 1}: Chave = [${chave}] | Valor = [${data[i][1]}]`);
      encontradas++;
    }
  }
  
  if(encontradas === 0) {
    console.error("❌ Nenhuma chave encontrada! Tem espaço em branco no nome?");
  }
  console.log("=== FIM DA INVESTIGAÇÃO ===");
}

// ============================================================================
// TESTES DE INTEGRAÇÃO (001)
// ============================================================================

function testConfigArchitectureV5() {
  ConfigManager.clearCache();
  const cfg = ConfigManager.get();
  const dudSize = Object.keys(SYS_CONFIG.DUD).length;
  
  console.log("=== HOMOLOGAÇÃO ARQUITETURA DE DADOS v5.0 ===");
  console.log(`Abas Mapeadas: ${Object.keys(SYS_CONFIG.SHEETS).length}`);
  console.log(`Dicionário DUD: ${dudSize} definições.`);
  console.log(`Chave SPOT: ${SYS_CONFIG.DUD["SPOT"]}`); // Deve retornar "spot"
  
  if(dudSize > 0) {
    console.log("Status: ✅ DUD Integrado e Pronto para o Web App.");
  } else {
    console.error("Status: ❌ Erro na carga do Dicionário.");
  }
}
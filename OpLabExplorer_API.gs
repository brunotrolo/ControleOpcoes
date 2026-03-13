/**
 * ═══════════════════════════════════════════════════════════════
 * OpLabExplorer_API.gs - MÓDULO ISOLADO
 * ═══════════════════════════════════════════════════════════════
 * RESPONSABILIDADE: Realizar chamadas de Proxy para a API OpLab
 * CONSUMO: Exclusivo para o componente OpLabExplorerView.html
 * ═══════════════════════════════════════════════════════════════
 */

/**
 * Executa uma requisição GET na API da OpLab.
 * Chamado pelo frontend via google.script.run.callOpLabAPI
 * * @param {string} path - Path do endpoint (ex: /market/options/PETR4)
 * @param {Object} queryParams - Objeto com query parameters
 * @returns {Object} { status, data, elapsed, url }
 */
function callOpLabAPI(path, queryParams) {
  const startTime = new Date().getTime();
  const OPLAB_BASE_URL = "https://api.oplab.com.br/v3";
  
  try {
    // 🛡️ Segurança: Busca o token salvo nas Propriedades do Script
    // Certifique-se de cadastrar a chave 'OPLAB_ACCESS_TOKEN' nas configurações do projeto
    const OPLAB_ACCESS_TOKEN = PropertiesService.getScriptProperties().getProperty("OPLAB_ACCESS_TOKEN");
    
    if (!OPLAB_ACCESS_TOKEN) {
      throw new Error("Configuração ausente: OPLAB_ACCESS_TOKEN não encontrado nas Propriedades do Script.");
    }

    // 1. Montagem da URL com Query Parameters
    let url = OPLAB_BASE_URL + path;
    
    if (queryParams && Object.keys(queryParams).length > 0) {
      const qs = Object.entries(queryParams)
        .filter(([_, v]) => v !== "" && v !== null && v !== undefined)
        .map(([k, v]) => encodeURIComponent(k) + "=" + encodeURIComponent(v))
        .join("&");
      if (qs) url += "?" + qs;
    }
    
    // 2. Configuração da Requisição
    const options = {
      method: "get",
      headers: {
        "Access-Token": OPLAB_ACCESS_TOKEN,
        "Accept": "application/json"
      },
      muteHttpExceptions: true
    };
    
    // 3. Execução via UrlFetchApp
    const response = UrlFetchApp.fetch(url, options);
    const elapsed = new Date().getTime() - startTime;
    const statusCode = response.getResponseCode();
    const responseText = response.getContentText();
    
    // 4. Tratamento do Payload (Tenta JSON, senão devolve texto)
    let data;
    try {
      data = JSON.parse(responseText);
    } catch (e) {
      data = responseText;
    }
    
    return {
      status: statusCode,
      data: data,
      elapsed: elapsed,
      url: url
    };
    
  } catch (error) {
    const elapsed = new Date().getTime() - startTime;
    return {
      status: "ERR",
      data: { error: error.message },
      elapsed: elapsed,
      url: OPLAB_BASE_URL + path
    };
  }
}

/**
 * Utilitário de Teste Rápido
 * Pode ser executado manualmente pelo editor do Apps Script
 */
function debug_testExplorerAPI() {
  const result = callOpLabAPI("/market/status", {});
  console.log("Status:", result.status);
  console.log("Resposta:", JSON.stringify(result.data).substring(0, 100) + "...");
}
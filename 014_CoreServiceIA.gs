/**
 * @fileoverview 012_CoreServiceIA - v5.0 (Gemini AI Engine)
 * AÇÃO: Motor centralizado para chamadas ao Google Gemini (Flash 2.5).
 * PADRÃO: Singleton (GeminiService), SysLogger integrado e Limpeza JSON Automática.
 */

const IA_CONFIG = {
  MODEL: "gemini-2.5-flash", // Modelo atualizado
  TEMPERATURE: 0.2,
  TOP_P: 0.8
};

const GeminiService = {
  _serviceName: "GeminiService_v5.0",

  /**
   * Chamada centralizada para a IA.
   * @param {string} promptText - O prompt do usuário.
   * @param {string} systemInstruction - (Opcional) Define a persona da IA.
   * @param {boolean} isJsonResponse - Se true, força e limpa o retorno para JSON.
   * @returns {string|object|null} A resposta limpa (Texto ou JSON).
   */
  generate(promptText, systemInstruction = "", isJsonResponse = false) {
    const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
    
    if (!apiKey) {
      SysLogger.log(this._serviceName, "ERRO_CONFIG", "GEMINI_API_KEY não encontrada nas Propriedades do Script.", "Vá em Project Settings > Script Properties e adicione.");
      SysLogger.flush();
      return null;
    }

    const url = `https://generativelanguage.googleapis.com/v1beta/models/${IA_CONFIG.MODEL}:generateContent?key=${apiKey}`;
    
    // Estrutura o payload (System Instruction separada para melhor obediência do modelo)
    const payload = {
      contents: [{ parts: [{ text: promptText }] }],
      generationConfig: {
        temperature: IA_CONFIG.TEMPERATURE,
        topP: IA_CONFIG.TOP_P,
        responseMimeType: isJsonResponse ? "application/json" : "text/plain"
      }
    };

    if (systemInstruction) {
      payload.systemInstruction = { parts: [{ text: systemInstruction }] };
    }

    const options = {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    };

    try {
      const response = UrlFetchApp.fetch(url, options);
      const resCode = response.getResponseCode();
      const resText = response.getContentText();
      
      if (resCode !== 200) {
        SysLogger.log(this._serviceName, "ERRO_API", `Status HTTP: ${resCode}`, resText);
        return null;
      }

      const json = JSON.parse(resText);
      if (!json.candidates || !json.candidates[0].content) {
         SysLogger.log(this._serviceName, "ERRO_FORMATO", "API não retornou o objeto de texto esperado.", resText);
         return null;
      }

      let rawContent = json.candidates[0].content.parts[0].text;

      // Sanitização de JSON blindada
      if (isJsonResponse) {
        rawContent = rawContent.replace(/```json/gi, "").replace(/```/g, "").trim();
        try {
          return JSON.parse(rawContent);
        } catch (e) {
          SysLogger.log(this._serviceName, "ERRO_PARSING", "Falha no parse inicial. Acionando Fallback Regex.", rawContent);
          const match = rawContent.match(/\[.*\]|\{.*\}/s);
          return match ? JSON.parse(match[0]) : null;
        }
      }

      return rawContent;

    } catch (e) {
      SysLogger.log(this._serviceName, "CRITICO", "Falha fatal na comunicação com o Gemini", String(e.message));
      SysLogger.flush();
      return null;
    }
  }
};

// ============================================================================
// PONTO DE COMPATIBILIDADE RETRÔ 
// ============================================================================

function callGeminiAI(promptText, systemInstruction = "", isJsonResponse = false) {
  return GeminiService.generate(promptText, systemInstruction, isJsonResponse);
}

// ============================================================================
// SUÍTE DE HOMOLOGAÇÃO E TESTES UNITÁRIOS (012)
// ============================================================================

/**
 * Executa uma bateria de testes sem mexer nas planilhas de dados.
 * Avalia Conexão, Parse Textual e Parse JSON.
 */
function testSuiteGeminiService012() {
  console.log("=== INICIANDO AUDITORIA: GEMINI SERVICE (012) ===");
  
  const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!apiKey) {
    console.error("❌ ERRO FATAL: GEMINI_API_KEY não configurada no Apps Script (Project Settings > Script Properties).");
    return;
  } else {
     console.log("✅ API KEY detectada (Tamanho: " + apiKey.length + " caracteres)");
  }

  // TESTE 1: TEXTO
  console.log(`\n--- TESTE 1: Conexão e Resposta Textual (${IA_CONFIG.MODEL}) ---`);
  const t0 = Date.now();
  const txt = GeminiService.generate("Responda exatamente apenas com a palavra 'SISTEMA_OK'.", "Você é um robô de testes bem direto e monossilábico.");
  const t1 = Date.now();
  
  if (txt && txt.includes("SISTEMA_OK")) {
    console.log(`✅ SUCESSO [Latência: ${t1 - t0}ms]: Resposta Textual Perfeita. Retorno: ${txt.trim()}`);
  } else {
    console.error(`❌ FALHA: Resposta incorreta ou nula. Retorno obtido: ${txt}`);
  }

  // TESTE 2: JSON
  console.log("\n--- TESTE 2: Conexão e Resposta JSON (Estruturada) ---");
  const t2 = Date.now();
  const promptJson = "Retorne um objeto JSON estrito contendo a chave 'motor' com valor 'GeminiFlash' e a chave 'versao' com valor numérico 5.";
  const obj = GeminiService.generate(promptJson, "Você é uma API que responde APENAS em formato JSON válido.", true);
  const t3 = Date.now();
  
  if (obj && typeof obj === 'object' && obj.motor === 'GeminiFlash') {
    console.log(`✅ SUCESSO [Latência: ${t3 - t2}ms]: Motor JSON parseou perfeitamente o objeto!`);
    console.log(JSON.stringify(obj, null, 2));
  } else {
    console.error(`❌ FALHA: O motor não conseguiu retornar ou parsear o JSON adequadamente.`, obj);
  }
  
  console.log("\n--- TESTE 3: Verificação do Logging ---");
  SysLogger.log("GeminiService_TEST", "INFO", "Validando integração entre IA e Sistema de Logs.", "");
  SysLogger.flush();
  console.log("✅ Teste finalizado. (Verifique a aba Logs para checar a integração).");

  console.log("=== FIM DA AUDITORIA ===");
}
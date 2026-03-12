/**
 * @fileoverview CoreSyncStockDataHistory - v4.1 (Sanitized & Audited)
 * AÇÃO: Busca série histórica e injeta Firewall de Dados antes do Drop & Replace.
 */

const HistoricalDataSync = {
  _serviceName: "HistoricalDataSync_v4.1",
  _abaDestino: SYS_CONFIG.SHEETS.HIST_250D, 
  _diasHistorico: 250,

  run() {
    const inicio = Date.now();
    
    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const abaAtivos = ss.getSheetByName(SYS_CONFIG.SHEETS.ASSETS); 
      
      if (!abaAtivos) throw new Error(`Aba origem (${SYS_CONFIG.SHEETS.ASSETS}) não encontrada.`);

      const headersAtivos = abaAtivos.getRange(1, 1, 1, abaAtivos.getLastColumn()).getValues()[0];
      const colTickerIdx = headersAtivos.indexOf("TICKER");
      if (colTickerIdx === -1) throw new Error("Coluna 'TICKER' não encontrada em DADOS_ATIVOS.");

      const maxRows = abaAtivos.getLastRow();
      if (maxRows < 2) return;

      const tickersBrutos = abaAtivos.getRange(2, colTickerIdx + 1, maxRows - 1, 1).getValues().flat();
      const tickersAlvo = [...new Set(tickersBrutos.filter(t => t && String(t).trim() !== ""))];

      if (!tickersAlvo.includes("IBOV")) tickersAlvo.push("IBOV");

      SysLogger.log(this._serviceName, "INFO", `Mapeados ${tickersAlvo.length} ativos p/ histórico.`, JSON.stringify(tickersAlvo));

      const bufferDeDados = [];
      let contagemErro = 0;
      let logsAuditoriaAPI = []; 

      tickersAlvo.forEach((ticker, index) => {
        const resAPI = OplabService.getHistoricalData(ticker, this._diasHistorico);
        
        if (resAPI && resAPI.data && resAPI.data.length > 0) {
          
          // 🚀 RAIO-X: Audita a estrutura do JSON recebido no primeiro ativo
          if (index === 0) {
            SysLogger.log(this._serviceName, "RAIO-X_API", `Dump histórico de ${ticker}`, JSON.stringify(resAPI.data[0]));
          }

          const symbolName = resAPI.symbol || ticker;
          const companyName = resAPI.name || "N/A";
          const resolution = resAPI.resolution || "1d";

          // 🛡️ A LAVANDERIA DE MASSA: Sanitiza cada um dos 250 candles
          const rows = resAPI.data.map(item => [
            Sanitizador.textoPuro(ticker),                  // TICKER
            new Date(),                                     // UPDATED_AT (Objeto nativo)
            Sanitizador.textoPuro(symbolName),              // SYMBOL_API
            Sanitizador.textoPuro(companyName),             // COMPANY_NAME
            Sanitizador.textoPuro(resolution),              // RESOLUTION
            Sanitizador.dataPura(item.time),                // 🚀 CANDLE_TIME (Data Pura)
            Sanitizador.numeroPuro(item.open),              // OPEN
            Sanitizador.numeroPuro(item.high),              // HIGH
            Sanitizador.numeroPuro(item.low),               // LOW
            Sanitizador.numeroPuro(item.close),             // SPOT
            Sanitizador.numeroPuro(item.volume),            // VOLUME_QTY
            Sanitizador.numeroPuro(item.fvolume)            // VOLUME_FIN
          ]);
          
          bufferDeDados.push(...rows);
        } else {
          contagemErro++;
          logsAuditoriaAPI.push(ticker);
        }
        if (index < tickersAlvo.length - 1) Utilities.sleep(800); 
      });

      if (logsAuditoriaAPI.length > 0) {
         SysLogger.log(this._serviceName, "ERRO_API", "Ativos sem histórico retornado:", logsAuditoriaAPI.join(", "));
      }

      if (bufferDeDados.length === 0) throw new Error("API retornou vazio. Abortando Drop & Replace.");

      // 3. DROP & REPLACE
      let abaDestino = ss.getSheetByName(this._abaDestino);
      if (!abaDestino) abaDestino = ss.insertSheet(this._abaDestino);

      const headersDUD = [
        "TICKER", "UPDATED_AT", "SYMBOL_API", "COMPANY_NAME", "RESOLUTION", 
        "CANDLE_TIME", "OPEN", "HIGH", "LOW", "SPOT", "VOLUME_QTY", "VOLUME_FIN"
      ];

      abaDestino.clearContents(); 
      abaDestino.getRange(1, 1, 1, headersDUD.length).setValues([headersDUD]);
      abaDestino.getRange(2, 1, bufferDeDados.length, headersDUD.length).setValues(bufferDeDados);

      // 🎨 APLICA MÁSCARAS VISUAIS DE MASSA
      abaDestino.getRange(2, 2, bufferDeDados.length).setNumberFormat('dd/MM/yyyy HH:mm:ss'); // Updated_At
      abaDestino.getRange(2, 6, bufferDeDados.length).setNumberFormat('dd/MM/yyyy'); // Candle_Time
      abaDestino.getRange(2, 7, bufferDeDados.length, 4).setNumberFormat('"$"#,##0.00'); // Preços (Open a Spot)
      abaDestino.getRange(2, 11, bufferDeDados.length, 2).setNumberFormat('#,##0'); // Volumes

      const duracao = ((Date.now() - inicio) / 1000).toFixed(1);
      SysLogger.log(this._serviceName, "FINISH", `>>> HISTÓRICO ATUALIZADO EM ${duracao}s <<<`, JSON.stringify({
        linhas: bufferDeDados.length,
        erros: contagemErro
      }));
      SysLogger.flush();

    } catch (e) {
      SysLogger.log(this._serviceName, "CRITICO", "Erro no Sync de Histórico", String(e.message));
      SysLogger.flush();
    }
  }
};

// ============================================================================
// PONTO DE ENTRADA (Trigger Dinâmico do Orquestrador)
// ============================================================================

/**
 * Ponto de entrada para varrer os ativos atuais e atualizar a série de 250 dias.
 */
function atualizarDadosHistoricos() {
  HistoricalDataSync.run();
}

// ============================================================================
// SUÍTE DE HOMOLOGAÇÃO 101% (009)
// ============================================================================

function testSuiteHistoricalSync009() {
  console.log("=== INICIANDO TESTE: HISTORICAL SYNC (009) ===");
  const tickerTeste = "IBOV"; 
  
  console.log(`--- Testando Conexão API para Histórico de ${tickerTeste} (5 dias) ---`);
  const t0 = Date.now();
  const resAPI = OplabService.getHistoricalData(tickerTeste, 5); 
  const t1 = Date.now();
  
  if (resAPI && resAPI.data && resAPI.data.length > 0) {
    console.log(`✅ [API] Retornou histórico em ${t1 - t0}ms.`);
    console.log(`   Último Candle (Data): ${resAPI.data[resAPI.data.length-1].time}`);
    console.log(`   Último Fechamento: ${resAPI.data[resAPI.data.length-1].close}`);
  } else {
    console.error(`❌ [API] Falha ao extrair histórico de ${tickerTeste}.`);
  }
  
  console.log("--- Executando Carga Total Controlada ---");
  HistoricalDataSync.run();
  
  console.log("=== TESTES CONCLUÍDOS ===");
}
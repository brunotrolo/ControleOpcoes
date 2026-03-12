/**
 * @fileoverview CoreSyncGreeks - v6.0 (Sanitized Risk Engine)
 * AÇÃO: Calcula e sincroniza Gregas via API Black-Scholes.
 * PROTEÇÃO: Dupla Sanitização (Pré-API e Pós-API) para evitar colapsos matemáticos.
 */

const GreeksSync = {
  _serviceName: "GreeksSync_v6.0",

  run() {
    const inicio = Date.now();
    const cacheBS = {}; 
    const stats = { lidos: 0, ativos: 0, gravados: 0, skip_status: 0, erros: 0, cache_hits: 0 };
    const statusEncontrados = {};
    const errosDetalhes = [];

    SysLogger.log(this._serviceName, "START", ">>> INICIANDO MOTOR DE GREGAS (Sanitizado) <<<", "");

    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const abaImport   = ss.getSheetByName(SYS_CONFIG.SHEETS.IMPORT);
      const abaGreeks   = ss.getSheetByName(SYS_CONFIG.SHEETS.GREEKS_API);
      const abaDetails  = ss.getSheetByName(SYS_CONFIG.SHEETS.DETAILS);
      const abaAssets   = ss.getSheetByName(SYS_CONFIG.SHEETS.ASSETS);
      
      if (!abaImport || !abaGreeks) throw new Error("Abas críticas não encontradas.");

      const detailsMap = this._getDynamicMap(abaDetails, "ID_TRADE");
      const assetsMap  = this._getDynamicMap(abaAssets, "TICKER");

      const colI = this._getColMap(abaImport);
      const colG = this._getColMap(abaGreeks);

      const idToRowMap = {};
      if (abaGreeks.getLastRow() > 1) {
        const ids = abaGreeks.getRange(2, colG.ID_TRADE + 1, abaGreeks.getLastRow() - 1, 1).getValues();
        ids.forEach((l, i) => { if (l[0]) idToRowMap[String(l[0]).trim()] = i + 2; });
      }

      const valoresImport = abaImport.getDataRange().getValues();

      for (let i = 1; i < valoresImport.length; i++) {
        const linha = valoresImport[i];
        const idTrade  = String(linha[colI.ID_TRADE] || "").trim();
        const optTicker = String(linha[colI.OPTION_TICKER] || "").trim();
        const statusUpper = String(linha[colI.STATUS_OP] || "").trim().toUpperCase();

        if (!idTrade || idTrade.length < 5) continue;
        
        stats.lidos++;
        statusEncontrados[statusUpper] = (statusEncontrados[statusUpper] || 0) + 1;

        if (statusUpper !== "ATIVO") { 
          stats.skip_status++; 
          continue; 
        }
        stats.ativos++;

        const detail = detailsMap[idTrade];
        const asset  = detail ? assetsMap[detail.TICKER] : null;

        if (!detail || !asset) {
          stats.erros++;
          errosDetalhes.push(`${optTicker} (Falta Info Ação/Detalhe)`);
          continue; 
        }

        let bsResult = null;
        if (cacheBS[optTicker]) {
          bsResult = cacheBS[optTicker];
          stats.cache_hits++;
        } else {
          // 🛡️ 1. SANITIZAÇÃO PRÉ-API (Evita enviar NaN ou Lixo para a OpLab)
          const params = {
            symbol: optTicker,
            irate: 10.75, 
            type: Sanitizador.textoPuro(detail.OPTION_TYPE),
            spotprice: Sanitizador.numeroPuro(asset.SPOT) || 1, // Fallback p/ não dividir por zero
            strike: Sanitizador.numeroPuro(detail.STRIKE) || 1,
            dtm: Sanitizador.numeroPuro(detail.DTE_CALENDAR) || 1,
            vol: Sanitizador.numeroPuro(asset.IV) || 30, // Fallback caso o IV da ação venha vazio
            amount: Math.abs(Sanitizador.numeroPuro(linha[colI.QUANTITY]) || 0)
          };

          bsResult = OplabService.calculateBS(params);
          if (bsResult) {
            cacheBS[optTicker] = bsResult;
            Utilities.sleep(850); 
          } else {
            stats.erros++;
            errosDetalhes.push(`${optTicker} (Falha no Cálculo BS)`);
          }
        }

        if (bsResult) {
          const rowNum = idToRowMap[idTrade];
          let linhaFinal = rowNum ? abaGreeks.getRange(rowNum, 1, 1, abaGreeks.getLastColumn()).getValues()[0] : new Array(abaGreeks.getLastColumn()).fill("");

          // 🛡️ 2. SANITIZAÇÃO PÓS-API (Limpa as gregas antes de injetar na planilha)
          const dadosLimpos = {
            ID_TRADE: Sanitizador.textoPuro(idTrade),
            OPTION_TICKER: Sanitizador.textoPuro(optTicker),
            ID_STRATEGY: Sanitizador.textoPuro(linha[colI.ID_STRATEGY]),
            UPDATED_AT: new Date(), // Data Objeto Absoluto
            DELTA: Sanitizador.numeroPuro(bsResult.delta),
            GAMMA: Sanitizador.numeroPuro(bsResult.gamma),
            VEGA: Sanitizador.numeroPuro(bsResult.vega),
            THETA: Sanitizador.numeroPuro(bsResult.theta),
            RHO: Sanitizador.numeroPuro(bsResult.rho),
            POE: Sanitizador.numeroPuro(bsResult.poe),
            PRICE: Sanitizador.numeroPuro(bsResult.price),
            IV_CALC: Sanitizador.numeroPuro(bsResult.volatility),
            MONEYNESS: Sanitizador.textoPuro(bsResult.moneyness_code || bsResult.moneyness),
            MONEYNESS_RATIO: Sanitizador.numeroPuro(bsResult.moneyness_ratio || (Sanitizador.numeroPuro(asset.SPOT) / Sanitizador.numeroPuro(detail.STRIKE))),
            SPOT: Sanitizador.numeroPuro(asset.SPOT),
            STRIKE: Sanitizador.numeroPuro(detail.STRIKE)
          };

          for (const label in colG) {
            const idx = colG[label];
            if (dadosLimpos[label] !== undefined) {
              linhaFinal[idx] = dadosLimpos[label];
            }
          }

          if (rowNum) {
            abaGreeks.getRange(rowNum, 1, 1, linhaFinal.length).setValues([linhaFinal]);
          } else {
            abaGreeks.appendRow(linhaFinal);
            idToRowMap[idTrade] = abaGreeks.getLastRow();
          }
          stats.gravados++;
        }
      }

      const duracaoFinal = ((Date.now() - inicio) / 1000).toFixed(1);
      
      // 🚀 LOG METICULOSO
      const payloadLog = {
        metricas: {
          total_linhas: stats.lidos,
          ignorados: stats.skip_status,
          ativos_calculados: stats.gravados,
          uso_de_cache: stats.cache_hits,
          falhas: stats.erros
        },
        erros_detalhe: errosDetalhes.length > 0 ? errosDetalhes : "Nenhum"
      };

      SysLogger.log(this._serviceName, "FINISH", `>>> GREGAS ATUALIZADAS EM ${duracaoFinal}s <<<`, JSON.stringify(payloadLog));
      SysLogger.flush();

    } catch (e) {
      SysLogger.log(this._serviceName, "CRITICO", "Falha fatal no Motor de Gregas", String(e.message));
      SysLogger.flush();
    }
  },

  _getColMap(aba) {
    if (!aba) return {};
    const headers = aba.getRange(1, 1, 1, aba.getLastColumn()).getValues()[0];
    const map = {};
    headers.forEach((h, i) => { if(h) map[String(h).trim().toUpperCase()] = i; });
    return map;
  },

  _getDynamicMap(aba, pkLabel) {
    if (!aba) return {};
    const data = aba.getDataRange().getValues();
    const headers = data[0];
    const pkIdx = headers.indexOf(pkLabel);
    if (pkIdx === -1) return {};
    const map = {};
    for (let i = 1; i < data.length; i++) {
      const obj = {};
      headers.forEach((h, idx) => obj[h] = data[i][idx]);
      if (data[i][pkIdx]) map[String(data[i][pkIdx]).trim()] = obj;
    }
    return map;
  }
};

// ============================================================================
// PONTO DE ENTRADA (Trigger Dinâmico / Menu)
// ============================================================================

/** Ponto de Entrada para o Motor de Risco */
function atualizarGregas() {
  GreeksSync.run();
}

// ============================================================================
// SUÍTE DE HOMOLOGAÇÃO (010)
// ============================================================================

/**
 * Testa o cálculo individual de Black-Scholes via API OpLab.
 */
function testSuiteGreeksSync010() {
  console.log("=== INICIANDO HOMOLOGAÇÃO: GREEKS SYNC (010) ===");
  
  const paramsTeste = {
    symbol: "PETRR315",
    irate: 10.75,      // Taxa Selic (em %)
    type: "PUT",       // Tipo da opção
    spotprice: 40.69,  // Preço atual do ativo objeto
    strike: 30.73,     // Preço de exercício
    dtm: 71,           // Dias úteis para o vencimento
    vol: 35.5,         // Volatilidade Implícita (em %)
    amount: 1000       // Quantidade da posição
  };

  console.log(`🚀 Solicitando cálculo Black-Scholes para ${paramsTeste.symbol}...`);
  
  const t0 = Date.now();
  const resultado = OplabService.calculateBS(paramsTeste);
  const t1 = Date.now();

  if (resultado && resultado.delta !== undefined) {
    console.log(`✅ SUCESSO: Resposta recebida em ${t1 - t0}ms.`);
    console.log(`📐 Delta: ${resultado.delta} (Exposição direcional)`);
    console.log(`📐 Gamma: ${resultado.gamma} (Aceleração do Delta)`);
    console.log(`📐 Theta: ${resultado.theta} (Decaimento temporal)`);
    console.log(`💰 Preço Teórico: R$ ${resultado.price}`);
  } else {
    console.error("❌ FALHA: A API não retornou cálculos válidos.");
  }
  
  console.log("--- Executando Carga Controlada ---");
  GreeksSync.run();

  console.log("=== TESTES CONCLUÍDOS ===");
}
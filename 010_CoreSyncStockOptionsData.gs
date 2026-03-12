/**
 * @fileoverview CoreSyncStockOptionsData - v6.0 (Sanitized & Audited)
 * AÇÃO: Sincroniza detalhes de opções com Mapeamento Rígido, Tradutor Epoch e Firewall.
 * PADRÃO: Dicionário Universal de Dados (v5.0).
 */

const OptionDetailsSync = {
  _serviceName: "OptionDetailsSync_v6.0",

  run() {
    const inicio = Date.now();
    const cacheAPI = {};
    const stats = { lidos: 0, processados: 0, skip_status: 0, api_calls: 0, erros: 0 };

    const tickersAtualizados = [];
    const tickersNovos = [];
    const tickersComErro = [];

    SysLogger.log(this._serviceName, "START", ">>> INICIANDO SINCRONIZAÇÃO DE DERIVATIVOS <<<", "");

    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const abaImport = ss.getSheetByName(SYS_CONFIG.SHEETS.IMPORT);
      const abaDetalhes = ss.getSheetByName(SYS_CONFIG.SHEETS.DETAILS);
      
      if (!abaImport || !abaDetalhes) throw new Error("Abas não encontradas.");
      
      const colI = this._getColMap(abaImport);
      const colD = this._getColMap(abaDetalhes);
      const idToRowMap = this._getIDRowMap(abaDetalhes, colD.ID_TRADE);
      
      const valoresImport = abaImport.getDataRange().getValues();

      // 1. MAPEAMENTO RÍGIDO (API Key -> Spreadsheet Label)
      const fieldMapper = {
        "symbol": "OPTION_TICKER",
        "parent_symbol": "TICKER",
        "name": "CONTRACT_DESC",
        "close": "CLOSE",
        "volume": "VOLUME_QTY",
        "financial_volume": "VOLUME_FIN",
        "trades": "TRADES_COUNT",
        "bid": "BID",
        "ask": "ASK",
        "due_date": "EXPIRY",
        "maturity_type": "MATURITY_TYPE",
        "contract_size": "LOT_SIZE",
        "exchange_id": "EXCHANGE_ID",
        "created_at": "CREATED_AT",
        "updated_at": "EDITED_AT", 
        "variation": "VARIATION",
        "spot_price": "SPOT",
        "isin": "ISIN",
        "security_category": "SECURITY_CAT",
        "market_maker": "MARKET_MAKER",
        "block_date": "BLOCK_DATE",
        "days_to_maturity": "DTE_CALENDAR",
        "cnpj": "CNPJ",
        "bid_volume": "BID_VOLUME",
        "ask_volume": "ASK_VOLUME",
        "time": "EXCH_TIMESTAMP",
        "type": "OPTION_TYPE",
        "last_trade_at": "LAST_TRADE_AT",
        "strike_eod": "STRIKE_EOD",
        "quotationForm": "QUOTATION_FORM",
        "lastUpdatedDividendsAt": "DIVIDEND_UPDATED_AT"
      };

      // 🛡️ REGRAS DE TIPAGEM DO FIREWALL
      const colunasDeTexto = [
        "ID_TRADE", "ID_STRATEGY", "OPTION_TICKER", "CONTRACT_DESC", "TICKER",
        "OPTION_TYPE", "CATEGORY", "MATURITY_TYPE", "EXCHANGE_ID", "ISIN",
        "CNPJ", "QUOTATION_FORM", "MARKET_MAKER"
      ];
      
      const colunasDeData = [
        "EXPIRY", "CREATED_AT", "EDITED_AT", "BLOCK_DATE", 
        "EXCH_TIMESTAMP", "LAST_TRADE_AT", "DIVIDEND_UPDATED_AT"
      ];

      const updatesEmLote = [];
      const listaParaNovos = [];

      for (let i = 1; i < valoresImport.length; i++) {
        const linhaImport = valoresImport[i];
        const idTrade = String(linhaImport[colI.ID_TRADE] || "").trim();
        const optTicker = String(linhaImport[colI.OPTION_TICKER] || "").trim();
        const status = String(linhaImport[colI.STATUS_OP] || "").trim().toUpperCase();

        if (!idTrade || idTrade.length < 5) continue;
        stats.lidos++;

        if (status !== "ATIVO") { stats.skip_status++; continue; }

        let dadosAPI = cacheAPI[optTicker] || null;
        if (!dadosAPI) {
          dadosAPI = OplabService.getOptionDetails(optTicker);
          if (dadosAPI) { 
            cacheAPI[optTicker] = dadosAPI; 
            stats.api_calls++; 
            Utilities.sleep(1100); 
          }
        }

        if (dadosAPI) {
          
          // 🚀 RAIO-X: Dump do primeiro ativo processado
          if (stats.processados === 0) {
            SysLogger.log(this._serviceName, "RAIO-X_API", `Dump detalhes de ${optTicker}`, JSON.stringify(dadosAPI));
          }

          const rowNum = idToRowMap[idTrade];
          let linhaFinal = rowNum ? abaDetalhes.getRange(rowNum, 1, 1, abaDetalhes.getLastColumn()).getValues()[0] : new Array(abaDetalhes.getLastColumn()).fill("");
          const chavesVaziasDaAPI = [];

          // 2. LÓGICA DE EXTRAÇÃO COM FIREWALL
          for (const label in colD) {
            const idx = colD[label];

            // A. Campos Controlados pelo Sistema
            if (label === "UPDATED_AT") {
              linhaFinal[idx] = new Date(); // Timestamp nativo absoluto
              continue;
            } else if (label === "ID_TRADE") {
              linhaFinal[idx] = Sanitizador.textoPuro(idTrade);
              continue;
            } else if (label === "ID_STRATEGY") {
              linhaFinal[idx] = Sanitizador.textoPuro(linhaImport[colI.ID_STRATEGY]);
              continue;
            }

            // B. Campos da API
            const apiKey = Object.keys(fieldMapper).find(key => fieldMapper[key] === label) || label.toLowerCase();
            let valorCru = dadosAPI[apiKey];

            if (valorCru === undefined || valorCru === null || valorCru === "") {
              chavesVaziasDaAPI.push(apiKey);
              continue;
            }

            // C. Lavanderia e Tipagem
            if (label === "EXPIRY") {
              // 🚀 VENCIMENTO: Força Objeto Data e zera a hora
              let d = Sanitizador.dataPura(valorCru);
              if (d instanceof Date) d.setHours(0, 0, 0, 0);
              linhaFinal[idx] = d;
              
            } else if (colunasDeData.includes(label)) {
              // 🚀 TRADUTOR EPOCH: Se for um número gigante, converte de Milissegundos para Data
              if (typeof valorCru === 'number' && valorCru > 1000000000) {
                linhaFinal[idx] = new Date(valorCru);
              } else {
                linhaFinal[idx] = Sanitizador.dataPura(valorCru);
              }
              
            } else if (colunasDeTexto.includes(label)) {
              linhaFinal[idx] = Sanitizador.textoPuro(valorCru);
              
            } else {
              linhaFinal[idx] = Sanitizador.numeroPuro(valorCru);
            }
          }

          // Auditoria de Falhas da B3/OpLab
          if (chavesVaziasDaAPI.length > 0) {
            SysLogger.log(this._serviceName, "FALTA_DADO", `Opção ${optTicker} retornou nulo para:`, chavesVaziasDaAPI.join(", "));
          }

          if (rowNum) {
            updatesEmLote.push({ linha: rowNum, dados: linhaFinal });
            tickersAtualizados.push(optTicker); // 🚀 RASTREIA ATUALIZAÇÕES
          } else {
            listaParaNovos.push(linhaFinal);
            tickersNovos.push(optTicker); // 🚀 RASTREIA NOVOS
          }
          
          stats.processados++;
        } else {
          stats.erros++;
          tickersComErro.push(optTicker); // 🚀 RASTREIA ERROS
          SysLogger.log(this._serviceName, "ERRO_API", `Falha ao buscar dados para a opção: ${optTicker}`, "");
        }
      }

      // 3. GRAVAÇÃO EM LOTE (Alta Performance)
      updatesEmLote.forEach(update => {
        abaDetalhes.getRange(update.linha, 1, 1, update.dados.length).setValues([update.dados]);
      });

      if (listaParaNovos.length > 0) {
        abaDetalhes.getRange(abaDetalhes.getLastRow() + 1, 1, listaParaNovos.length, listaParaNovos[0].length).setValues(listaParaNovos);
      }

      const duracao = ((Date.now() - inicio) / 1000).toFixed(1);
      
      const payloadAuditoria = {
        metricas_gerais: {
          total_linhas_lidas: stats.lidos,
          ignorados_nao_ativos: stats.skip_status,
          chamadas_reais_api: stats.api_calls,
          sucessos: stats.processados,
          falhas: stats.erros
        },
        detalhamento: {
          novos_inseridos: tickersNovos.length > 0 ? tickersNovos : "Nenhum",
          atualizados: tickersAtualizados.length > 0 ? tickersAtualizados : "Nenhum",
          erros_api: tickersComErro.length > 0 ? tickersComErro : "Nenhum"
        }
      };

      SysLogger.log(this._serviceName, "FINISH", `>>> SINCRONIA CONCLUÍDA EM ${duracao}s <<<`, JSON.stringify(payloadAuditoria));
      SysLogger.flush();
      

    } catch (e) {
      SysLogger.log(this._serviceName, "CRITICO", "Falha fatal no motor 008", String(e.message));
      SysLogger.flush();
    }
  },

  _getColMap(aba) {
    const headers = aba.getRange(1, 1, 1, aba.getLastColumn()).getValues()[0];
    const map = {};
    headers.forEach((h, i) => { if(h) map[String(h).trim().toUpperCase()] = i; }); 
    return map;
  },

  _getIDRowMap(aba, colIdx) {
    const map = {};
    if (aba.getLastRow() < 2 || colIdx === undefined) return map;
    const ids = aba.getRange(2, colIdx + 1, aba.getLastRow() - 1, 1).getValues();
    ids.forEach((l, i) => { if (l[0]) map[String(l[0]).trim()] = i + 2; });
    return map;
  }
};

// ============================================================================
// PONTO DE ENTRADA (Trigger Dinâmico / Menu)
// ============================================================================

/**
 * Ponto de entrada para sincronizar detalhes (Gregas, Strikes, etc) das Opções ativas.
 */
function atualizarDetalhesOpcoes() {
  OptionDetailsSync.run();
}

// ============================================================================
// SUÍTE DE HOMOLOGAÇÃO (008)
// ============================================================================

function testSuiteOptionDetailsSync008() {
  console.log("=== INICIANDO HOMOLOGAÇÃO: OPTION DETAILS SYNC (008) ===");
  const tickerTeste = "PETRC425"; // Ajuste para um ticker válido de opção se necessário
  
  console.log(`--- Testando Fetch da API para ${tickerTeste} ---`);
  const dados = OplabService.getOptionDetails(tickerTeste);
  
  if (dados && dados.strike) {
    console.log(`✅ Dados da Opção recebidos. Strike: ${dados.strike}`);
    console.log(`   Data de Vencimento Original: ${dados.due_date}`);
  } else {
    console.error(`❌ Falha ao processar ${tickerTeste}. Talvez o ativo não exista mais ou a API falhou.`);
  }

  console.log("--- Executando Carga Controlada ---");
  OptionDetailsSync.run();

  console.log("=== TESTES CONCLUÍDOS ===");
}
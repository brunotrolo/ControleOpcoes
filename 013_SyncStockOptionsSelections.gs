/**
 * @fileoverview 101_CoreScannerOptions - v6.0 (Sanitized Master Scanner)
 * OBJETIVO: Varre o mercado buscando opções próximas ao Spot com Firewall de Dados.
 * PADRÃO: Dicionário Universal de Dados (v5.0) + Localidade Independente.
 */

const CoreScannerOptions = {
  _serviceName: "CoreScanner_v6.0",

  run() {
    const inicio = Date.now();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // 1. LEITURA DE REGRAS (Config_Global)
    const abaConfig = ss.getSheetByName("Config_Global");
    const dataConfig = abaConfig.getDataRange().getValues();
    const config = {};
    dataConfig.forEach(row => { if (row[0]) config[String(row[0]).trim()] = row[1]; });
    
    const vencimentoAlvo = config["Regra_Vencimento_Entrada_Opcoes"];
    const qtdMaxPUT      = parseInt(config["Regra_Qtd_Max_PUT"] || 10);
    const qtdMaxCALL     = parseInt(config["Regra_Qtd_Max_CALL"] || 10);

    SysLogger.log(this._serviceName, "START", ">>> INICIANDO RADAR <<<", `Venc: ${vencimentoAlvo} | Teto: P:${qtdMaxPUT} C:${qtdMaxCALL}`);

    if (!vencimentoAlvo) {
      SysLogger.log(this._serviceName, "ERRO_CRITICO", "Regra de vencimento vazia na Config_Global.", "");
      SysLogger.flush();
      return;
    }

    try {
      const abaAtivos = ss.getSheetByName(SYS_CONFIG.SHEETS.ASSETS);
      const abaSaida  = ss.getSheetByName(SYS_CONFIG.SHEETS.SELECTION_OPT);
      
      if (!abaAtivos || !abaSaida) throw new Error("Abas críticas não encontradas.");

      const tickers = this._getTickersAlvo(abaAtivos);
      if (tickers.length === 0) return;

      // 3. LIMPEZA SEGURA
      const lastRowSaida = abaSaida.getLastRow();
      if (lastRowSaida > 1) {
        abaSaida.getRange(2, 1, lastRowSaida - 1, abaSaida.getLastColumn()).clearContent();
      }

      const headersOut = abaSaida.getRange(1, 1, 1, abaSaida.getLastColumn()).getValues()[0];
      let bufferFinal = [];
      const stats = { ativos: 0, puts: 0, calls: 0, erros: 0 };

      // 4. VARREDURA
      tickers.forEach((ticker, index) => {
          const opcoesAPI = OplabService.getOptionsByTicker(ticker);

        if (!opcoesAPI || !Array.isArray(opcoesAPI) || opcoesAPI.length === 0) {
          stats.erros++;
          return;
        }

        // Filtro de Vencimento e Liquidez básica
        const filtradas = opcoesAPI.filter(op => op.due_date === vencimentoAlvo && op.spot_price > 0);
        if (filtradas.length === 0) return;

        const spot = Sanitizador.numeroPuro(filtradas[0].spot_price);
        stats.ativos++;

        // Lógica de Strangles
        let puts = filtradas.filter(op => op.category === "PUT" && op.strike < spot);
        let calls = filtradas.filter(op => op.category === "CALL" && op.strike > spot);

        // Ordenação por proximidade ao Spot
        puts.sort((a, b) => Math.abs(spot - a.strike) - Math.abs(spot - b.strike));
        calls.sort((a, b) => Math.abs(spot - a.strike) - Math.abs(spot - b.strike));

        // Limite e Reordenação Estética
        let putsFinal = puts.slice(0, qtdMaxPUT).sort((a, b) => a.strike - b.strike);
        let callsFinal = calls.slice(0, qtdMaxCALL).sort((a, b) => a.strike - b.strike);

        stats.puts += putsFinal.length;
        stats.calls += callsFinal.length;

        const selecionadas = [...putsFinal, ...callsFinal];
        selecionadas.forEach(op => {
          bufferFinal.push(this._mapearParaDUD(ticker, op, spot, headersOut));
        });
        if (index < tickers.length - 1) Utilities.sleep(700);
      });

      // 5. GRAVAÇÃO EM LOTE
      if (bufferFinal.length > 0) {
        abaSaida.getRange(2, 1, bufferFinal.length, headersOut.length).setValues(bufferFinal);
      }

      const duracao = ((Date.now() - inicio) / 1000).toFixed(1);
      SysLogger.log(this._serviceName, "FINISH", `>>> SCANNER CONCLUÍDO EM ${duracao}s <<<`, JSON.stringify({
        ativos_analisados: tickers.length,
        total_puts: stats.puts,
        total_calls: stats.calls,
        linhas_gravadas: bufferFinal.length
      }));
      SysLogger.flush();

    } catch (e) {
      SysLogger.log(this._serviceName, "CRITICO", "Falha no motor do Scanner", String(e.message));
      SysLogger.flush();
    }
  },

  _getTickersAlvo(aba) {
    const headers = aba.getRange(1, 1, 1, aba.getLastColumn()).getValues()[0];
    const idx = headers.indexOf("TICKER");
    if (idx === -1) return [];
    return aba.getRange(2, idx + 1, aba.getLastRow() - 1, 1).getValues().flat().filter(t => t);
  },

  /** * Helper 2: Mapeamento Blindado e Tipado (v6.1)
   */
  _mapearParaDUD(ticker, op, spotReal, headers) {
    // 🛡️ O MAPA DEFINITIVO DE TIPAGEM
    const map = {
      "TICKER": Sanitizador.textoPuro(ticker),
      "OPTION_TICKER": Sanitizador.textoPuro(op.symbol),
      "CONTRACT_DESC": Sanitizador.textoPuro(op.name),
      "CATEGORY": Sanitizador.textoPuro(op.category),
      "STYLE": Sanitizador.textoPuro(op.maturity_type),
      "TYPE": Sanitizador.textoPuro(op.category),
      
      // Matemática Lavada
      "OPEN": Sanitizador.numeroPuro(op.open),
      "HIGH": Sanitizador.numeroPuro(op.high),
      "LOW": Sanitizador.numeroPuro(op.low),
      "SPOT": Sanitizador.numeroPuro(spotReal),
      "VOLUME_QTY": Sanitizador.numeroPuro(op.volume),
      "VOLUME_FIN": Sanitizador.numeroPuro(op.financial_volume),
      "TRADES": Sanitizador.numeroPuro(op.trades),
      "BID": Sanitizador.numeroPuro(op.bid),
      "ASK": Sanitizador.numeroPuro(op.ask),
      "STRIKE": Sanitizador.numeroPuro(op.strike),
      "LOT_SIZE": Sanitizador.numeroPuro(op.contract_size),
      "VARIATION": Sanitizador.numeroPuro(op.variation),
      "DTE_CALENDAR": Sanitizador.numeroPuro(op.days_to_maturity),
      "STRIKE_EOD": Sanitizador.numeroPuro(op.strike_eod),
      "SPOT_PRICE_API": Sanitizador.numeroPuro(op.spot_price),
      "ISIN": Sanitizador.textoPuro(op.isin),
      "SECURITY_CAT": Sanitizador.numeroPuro(op.security_category),
      "MM_FLAG": op.market_maker ? "TRUE" : "FALSE",
      "CNPJ": Sanitizador.textoPuro(op.cnpj),

      // Datas Nativa
      "EXPIRY": (() => { 
          let d = Sanitizador.dataPura(op.due_date); 
          if(d instanceof Date) d.setHours(0,0,0,0); 
          return d; 
      })(),
      "CREATED_AT": Sanitizador.dataPura(op.created_at),
      "UPDATED_AT": new Date(), // Timestamp Absoluto
      "BLOCK_DATE": Sanitizador.dataPura(op.block_date),      
      "LAST_TRADE": (() => {
          const val = op.last_trade_at;
          if (!val || typeof val !== 'number' || val <= 0) return "";          
          if (val > 946684800000) return new Date(val);
          return Sanitizador.dataPura(val);
      })()
    };

    return headers.map(h => {
      const label = String(h).trim().toUpperCase();
      // Retorna o valor mapeado ou tenta buscar na API se o nome for idêntico
      if (map[label] !== undefined) return map[label];
      const valAPI = op[label.toLowerCase()];
      return (valAPI !== undefined && valAPI !== null) ? valAPI : "";
    });
  }
};

// ============================================================================
// PONTO DE ENTRADA (Trigger Dinâmico / Menu)
// ============================================================================

function atualizarScannerOpcoes() {
  CoreScannerOptions.run();
}

// ============================================================================
// SUÍTE DE TESTES (011)
// ============================================================================

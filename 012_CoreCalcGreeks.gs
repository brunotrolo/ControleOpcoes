/**
 * @fileoverview CoreCalcGreeks - v6.0 (Sanitized In-House Math Engine)
 * AÇÃO: Calcula Gregas e IV internamente via Black-Scholes (Newton-Raphson).
 * PROTEÇÃO: Sanitização estrita de Insumos para proteger a Matemática Pura.
 * PERFORMANCE: Escrita em Batch (Lote) para evitar limite de tempo do Google.
 */

const OptionMath = {
  DIAS_ANO: 252,
  T_MIN: 0.002, 

  pdf(x) { return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI); },

  cdf(x) {
    const t = 1 / (1 + 0.2316419 * Math.abs(x));
    const d = 0.3989423 * Math.exp(-x * x / 2);
    const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
    return x > 0 ? 1 - p : p;
  },

  calculate(S, K, T, r, sigma, flag) {
    T = Math.max(T, this.T_MIN);
    const sqrtT = Math.sqrt(T);
    const d1 = (Math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT);
    const d2 = d1 - (sigma * sqrtT);
    
    const nd1 = this.pdf(d1);
    const Nd1 = this.cdf(d1);
    const Nd2 = this.cdf(d2);
    const expRT = Math.exp(-r * T);

    const isCall = (flag.toLowerCase() === 'c' || flag.toLowerCase() === 'call');
    
    return {
      price: isCall ? (S * Nd1 - K * expRT * Nd2) : (K * expRT * this.cdf(-d2) - S * this.cdf(-d1)),
      delta: isCall ? Nd1 : Nd1 - 1,
      gamma: nd1 / (S * sigma * sqrtT),
      vega: (S * nd1 * sqrtT) / 100, 
      theta: (isCall ? 
              (-(S * nd1 * sigma) / (2 * sqrtT) - r * K * expRT * Nd2) :
              (-(S * nd1 * sigma) / (2 * sqrtT) + r * K * expRT * this.cdf(-d2))) / this.DIAS_ANO,
      rho: (isCall ? (K * T * expRT * Nd2) : (-K * T * expRT * this.cdf(-d2))) / 100,
      poe: isCall ? Nd2 : this.cdf(-d2)
    };
  },

  estimateIV(S, K, T, r, marketPrice, flag) {
    let sigma = 0.35; 
    for (let i = 0; i < 50; i++) {
      const g = this.calculate(S, K, T, r, sigma, flag);
      const diff = g.price - marketPrice;
      if (Math.abs(diff) < 0.0001) return sigma;
      const v = g.vega * 100; 
      if (v < 0.0001) break;
      sigma -= diff / v;
      if (sigma < 0.01) return 0.01;
      if (sigma > 5.0) return 5.0;
    }
    return sigma;
  },

  getMoneynessCode(S, K, flag) {
    const ratio = S / K;
    if (ratio >= 0.975 && ratio <= 1.025) return 'ATM';
    const isCall = (flag.toLowerCase() === 'c' || flag.toLowerCase() === 'call');
    if ((isCall && ratio > 1.025) || (!isCall && ratio < 0.975)) return 'ITM';
    return 'OTM';
  }
};

const GreeksCalculator = {
  _serviceName: "GreeksCalculator_v6.0",

  run() {
    const inicio = Date.now();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    const cacheCalculos = {};
    const stats = { lidos: 0, ativos: 0, gravados: 0, skip_status: 0, erros: 0, cache_hits: 0 };
    const errosDetalhes = [];

    const tickersAtualizados = [];
    const tickersNovos = [];

    SysLogger.log(this._serviceName, "START", ">>> INICIANDO CÁLCULO NATIVO (BS) <<<", "");

    try {
      const abaImport = ss.getSheetByName(SYS_CONFIG.SHEETS.IMPORT);
      const abaCalc = ss.getSheetByName(SYS_CONFIG.SHEETS.GREEKS_CALC);
      const abaDetails = ss.getSheetByName(SYS_CONFIG.SHEETS.DETAILS);
      const abaAssets = ss.getSheetByName(SYS_CONFIG.SHEETS.ASSETS);

      if (!abaCalc || !abaImport) throw new Error("Aba IMPORT ou CALC_GREEKS não encontrada.");

      const colI = this._getColMap(abaImport);
      const colC = this._getColMap(abaCalc);
      
      const detailsMap = this._getDynamicMap(abaDetails, "ID_TRADE");
      const assetsMap = this._getDynamicMap(abaAssets, "TICKER");
      
      const idToRowMap = {};
      if (abaCalc.getLastRow() > 1) {
        const ids = abaCalc.getRange(2, colC.ID_TRADE + 1, abaCalc.getLastRow() - 1, 1).getValues();
        ids.forEach((l, i) => { if (l[0]) idToRowMap[String(l[0]).trim()] = i + 2; });
      }

      const valoresImport = abaImport.getDataRange().getValues();
      const irate = 0.1075; // Taxa Selic fixada

      const updatesEmLote = [];
      const listaParaNovos = [];

      for (let i = 1; i < valoresImport.length; i++) {
        const linha = valoresImport[i];
        const idTrade = String(linha[colI.ID_TRADE] || "").trim();
        const optTicker = String(linha[colI.OPTION_TICKER] || "").trim();
        const statusUpper = String(linha[colI.STATUS_OP] || "").trim().toUpperCase();
        
        if (!idTrade || idTrade.length < 5) continue;
        stats.lidos++;

        if (statusUpper !== "ATIVO") { 
          stats.skip_status++; 
          continue; 
        }
        stats.ativos++;

        const detail = detailsMap[idTrade];
        const asset = detail ? assetsMap[detail.TICKER] : null;

        if (!detail || !asset) {
          stats.erros++;
          errosDetalhes.push(`${optTicker} (Falta Insumos)`);
          continue;
        }

        let resBS = cacheCalculos[optTicker] || null;

        if (!resBS) {
          // 🛡️ O FIREWALL MATEMÁTICO: Impede NaN de entrar no cálculo
          const S = Sanitizador.numeroPuro(asset.SPOT) || 1;
          const K = Sanitizador.numeroPuro(detail.STRIKE) || 1;
          const T_dias = Sanitizador.numeroPuro(detail.DTE_CALENDAR) || 1;
          const T_anos = T_dias / OptionMath.DIAS_ANO;
          const flag = String(detail.OPTION_TYPE || "c").toLowerCase() === 'call' ? 'c' : 'p';
          const precoMercado = Sanitizador.numeroPuro(detail.CLOSE) || 0.01;
          
          try {
            const iv = OptionMath.estimateIV(S, K, T_anos, irate, precoMercado, flag);
            resBS = OptionMath.calculate(S, K, T_anos, irate, iv, flag);
            resBS.volatility = iv;
            resBS.moneyness_code = OptionMath.getMoneynessCode(S, K, detail.OPTION_TYPE);
            resBS.moneyness_val = S / K;
            
            cacheCalculos[optTicker] = resBS;
          } catch (mathErr) {
            stats.erros++;
            errosDetalhes.push(`${optTicker} (Erro Newton-Raphson)`);
            continue;
          }
        }

        if (resBS) {
          const rowNum = idToRowMap[idTrade];
          let linhaFinal = rowNum ? abaCalc.getRange(rowNum, 1, 1, abaCalc.getLastColumn()).getValues()[0] : new Array(abaCalc.getLastColumn()).fill("");

          // 🛡️ MAPEAMENTO ABSOLUTO COM SANITIZAÇÃO PÓS-CÁLCULO
          const dadosMapeados = {
            ID_TRADE: Sanitizador.textoPuro(idTrade),
            OPTION_TICKER: Sanitizador.textoPuro(optTicker),
            ID_STRATEGY: Sanitizador.textoPuro(linha[colI.ID_STRATEGY]),
            UPDATED_AT: new Date(), // Timestamp Absoluto Nativo
            DELTA: Sanitizador.numeroPuro(resBS.delta),
            GAMMA: Sanitizador.numeroPuro(resBS.gamma),
            VEGA: Sanitizador.numeroPuro(resBS.vega),
            THETA: Sanitizador.numeroPuro(resBS.theta),
            RHO: Sanitizador.numeroPuro(resBS.rho),
            POE: Sanitizador.numeroPuro(resBS.poe),
            PRICE: Sanitizador.numeroPuro(resBS.price),
            IV_CALC: Sanitizador.numeroPuro(resBS.volatility),
            MONEYNESS: Sanitizador.textoPuro(resBS.moneyness_code),
            MONEYNESS_RATIO: Sanitizador.numeroPuro(resBS.moneyness_val),
            SPOT: Sanitizador.numeroPuro(asset.SPOT),
            STRIKE: Sanitizador.numeroPuro(detail.STRIKE)
          };

          for (const label in colC) {
            const idx = colC[label];
            if (dadosMapeados[label] !== undefined) {
              linhaFinal[idx] = dadosMapeados[label];
            }
          }

          if (rowNum) {
            updatesEmLote.push({ linha: rowNum, dados: linhaFinal });
            tickersAtualizados.push(optTicker); // 🚀 RASTREIA ATUALIZAÇÕES
          } else {
            listaParaNovos.push(linhaFinal);
            tickersNovos.push(optTicker); // 🚀 RASTREIA NOVOS
          }
          stats.gravados++;
        }
      }

      // 🚀 INJEÇÃO DE ALTA PERFORMANCE (BATCH WRITING)
      updatesEmLote.forEach(update => {
        abaCalc.getRange(update.linha, 1, 1, update.dados.length).setValues([update.dados]);
      });

      if (listaParaNovos.length > 0) {
        abaCalc.getRange(abaCalc.getLastRow() + 1, 1, listaParaNovos.length, listaParaNovos[0].length).setValues(listaParaNovos);
      }

      const duracaoFinal = ((Date.now() - inicio) / 1000).toFixed(1);

      const payloadLog = {
        metricas_gerais: {
          total_linhas_lidas: stats.lidos,
          ignorados_nao_ativos: stats.skip_status,
          ativos_calculados: stats.gravados,
          uso_de_cache: stats.cache_hits,
          falhas: stats.erros
        },
        detalhamento: {
          novos_inseridos: tickersNovos.length > 0 ? tickersNovos : "Nenhum",
          atualizados: tickersAtualizados.length > 0 ? tickersAtualizados : "Nenhum",
          erros_matematicos: errosDetalhes.length > 0 ? errosDetalhes : "Nenhum"
        }
      };

      SysLogger.log(this._serviceName, "FINISH", `>>> CÁLCULO NATIVO CONCLUÍDO EM ${duracaoFinal}s <<<`, JSON.stringify(payloadLog));
      SysLogger.flush();

    } catch (e) {
      SysLogger.log(this._serviceName, "CRITICO", "Falha catastrófica no motor nativo", String(e.message));
      SysLogger.flush();
    }
  },

  _getColMap(aba) {
    if (!aba) return {};
    const headers = aba.getRange(1, 1, 1, aba.getLastColumn()).getValues()[0];
    const map = {};
    headers.forEach((h, i) => { if(h) map[String(h).trim().toUpperCase()] = i; }); // Index 0-based
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


/** Função para o Orquestrador rodar o cálculo nativo */
function calcularGregasNativo() { 
  GreeksCalculator.run(); 
}

// ============================================================================
// SUÍTE DE TESTES (011)
// ============================================================================

function testSuiteCalcGreeksInternal011() {
  console.log("=== INICIANDO AUDITORIA MATEMÁTICA: CALC GREEKS (011) ===");
  const tol = 0.001;
  
  // Teste 1: CDF
  const cdfZero = OptionMath.cdf(0);
  console.log(`[MATH] CDF(0): ${cdfZero} ${Math.abs(cdfZero - 0.5) < tol ? "✅" : "❌"}`);

  // Teste 2: Black-Scholes ATM Call
  const S = 100, K = 100, T = 1, r = 0.05, vol = 0.20;
  const res = OptionMath.calculate(S, K, T, r, vol, 'c');
  console.log(`[BS] Preço: ${res.price.toFixed(2)} (Esperado: ~10.45)`);

  console.log("--- Executando Carga Controlada ---");
  GreeksCalculator.run();

  console.log("=== FIM DA AUDITORIA ===");
}
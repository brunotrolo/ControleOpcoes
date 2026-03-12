/**
 * @fileoverview CoreUpdatePortfolio - v4.2.1 (Bulletproof Edition)
 * AÇÃO: Sincronia de ativos com Lavanderia de Dados.
 * CORREÇÃO: Remoção de colunas calculadas para proteger as ArrayFormulas do banco.
 */

const PortfolioUpdater = {
  _serviceName: "PortfolioUpdater_v4.2.1",

  syncPortfolioData() {
    const inicio = Date.now();
    
    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const aba = ss.getSheetByName(SYS_CONFIG.SHEETS.IMPORT);
      if (!aba) throw new Error(`Aba não encontrada: ${SYS_CONFIG.SHEETS.IMPORT}`);

      const maxRows = aba.getLastRow();
      if (maxRows < 2) {
        SysLogger.log(this._serviceName, "AVISO", "Aba vazia ou apenas cabeçalho.", "Linhas: " + maxRows);
        return;
      }

      // 1. SCAN DINÂMICO DE CABEÇALHOS
      const headers = aba.getRange(1, 1, 1, aba.getLastColumn()).getValues()[0];
      const col = {};
      headers.forEach((label, index) => {
        if (label) col[String(label).trim().toUpperCase()] = index + 1;
      });

      // Validação de colunas obrigatórias
      const req = ["OPTION_TICKER", "ID_TRADE", "TICKER", "STATUS_OP"];
      req.forEach(key => {
        if (!col[key]) throw new Error(`Coluna obrigatória '${key}' não encontrada na aba.`);
      });

      const dataFull = aba.getRange(2, 1, maxRows - 1, aba.getLastColumn()).getValues();
      const linhasParaProcessar = [];
      const tickersSucesso = [];
      let contagemErro = 0;

      // 2. FASE DE MAPEAMENTO
      for (let i = 0; i < dataFull.length; i++) {
        const linhaPlanilha = i + 2; 
        const rowData = dataFull[i];
        
        const optionTicker = Sanitizador.textoPuro(rowData[col["OPTION_TICKER"] - 1]);
        const idTrade      = rowData[col["ID_TRADE"] - 1];         
        const jaEnriquecido = rowData[col["TICKER"] - 1]; 

        if (optionTicker && idTrade && !jaEnriquecido) {
          linhasParaProcessar.push({ linha: linhaPlanilha, optionTicker: optionTicker });
        }
      }

      SysLogger.log(this._serviceName, "INFO", `Mapeamento: ${linhasParaProcessar.length} ativos pendentes.`, `Total analisado: ${maxRows}`);

      if (linhasParaProcessar.length === 0) {
        SysLogger.log(this._serviceName, "FINISH", ">>> CICLO ENCERRADO: Nada para enriquecer. <<<");
        SysLogger.flush();
        return;
      }

      // 3. FASE DE EXECUÇÃO (LOOP BLINDADO)
      linhasParaProcessar.forEach((item) => {
        try {
          const dadosNovos = this._fetchOptionData(item.optionTicker);
          
          if (dadosNovos) {
            // A) GRAVA OS DADOS ENRIQUECIDOS PUROS
            const rangeEnriquecido = aba.getRange(item.linha, col["TICKER"], 1, 5);
            rangeEnriquecido.setValues([dadosNovos]);
            
            // B) APLICA MÁSCARA VISUAL NO STRIKE E VENCIMENTO
            try {
              aba.getRange(item.linha, col["TICKER"] + 2).setNumberFormat('"R$ "#,##0.00'); 
              aba.getRange(item.linha, col["TICKER"] + 1).setNumberFormat('dd/MM/yyyy');
            } catch(eVisual) { }

            // C) NORMALIZA OS DADOS SUJOS DA CORRETORA (LAVANDERIA ATIVA)
            this._normalizarDadosImportados(aba, item.linha, col);

            tickersSucesso.push(item.optionTicker);
            SysLogger.log(this._serviceName, "SUCESSO", `Linha ${item.linha}: ${item.optionTicker} normalizada.`, JSON.stringify(dadosNovos));
          } else {
            aba.getRange(item.linha, col["TICKER"], 1, 1).setValue("ERRO_API");
            contagemErro++;
            SysLogger.log(this._serviceName, "ERRO", `Falha na API para ${item.optionTicker}`, "Retornou null");
          }
        } catch (erroLinha) {
          contagemErro++;
          SysLogger.log(this._serviceName, "ERRO_CRITICO", `Falha fatal na linha ${item.linha}`, erroLinha.message);
        }
        
        if (linhasParaProcessar.length > 5) Utilities.sleep(600); 
      });

      const duracao = ((Date.now() - inicio) / 1000).toFixed(1);
      SysLogger.log(this._serviceName, "FINISH", `>>> CICLO FINALIZADO EM ${duracao}s <<<`, JSON.stringify({
        total: linhasParaProcessar.length,
        sucesso: tickersSucesso.length,
        erros: contagemErro
      }));
      SysLogger.flush(); 

    } catch (e) {
      SysLogger.log(this._serviceName, "CRITICO", "FALHA NO MOTOR 006", String(e.message));
      SysLogger.flush();
    }
  },

    /**
     * Busca dados na API e garante que o Vencimento venha sem hora.
     */
    _fetchOptionData(optionTicker) {
      try {
        const data = OplabService.getOptionDetails(optionTicker);
        if (!data) return null;

        // 🚀 ZERANDO A HORA: Captura a data e reseta o relógio para 00:00:00
        let vencimentoRaw = Sanitizador.dataPura(data.due_date || data.expiration);
        if (vencimentoRaw instanceof Date) {
          vencimentoRaw.setHours(0, 0, 0, 0); 
        }

        return [
          Sanitizador.textoPuro(data.parent_symbol || data.symbol),
          vencimentoRaw, // Agora é um objeto Date purificado (apenas dia/mês/ano)
          Sanitizador.numeroPuro(data.strike),                     
          Sanitizador.textoPuro(data.category || data.type),
          "ATIVO" 
        ];
      } catch (e) {
        return null;
      }
    },

    /**
    * 🧹 NORMALIZAÇÃO RETROATIVA: Varre os dados que você colou da corretora.
    */
    _normalizarDadosImportados(aba, linha, colMap) {
      const colunasAlvo = [
        { nome: "ENTRY_PRICE",  tipo: "numero", mascara: '"R$ "#,##0.00' },
        { nome: "LAST_PREMIUM", tipo: "numero", mascara: '"R$ "#,##0.00' },
        { nome: "LIMIT_PRICE",  tipo: "numero", mascara: '"R$ "#,##0.00' },
        { nome: "STRIKE",       tipo: "numero", mascara: '"R$ "#,##0.00' },
        { nome: "QUANTITY",     tipo: "numero", mascara: '#,##0' },
        { nome: "ORDER_DATE",   tipo: "data",   mascara: 'dd/MM/yyyy HH:mm:ss' },
        { nome: "EXPIRY",       tipo: "data",   mascara: 'dd/MM/yyyy' } 
      ];

      colunasAlvo.forEach(alvo => {
        const colIndex = colMap[alvo.nome];
        if (colIndex) {
          const range = aba.getRange(linha, colIndex);
          
          if (range.getFormula() !== "") return;

          const valorBruto = range.getValue();
          
          if (valorBruto !== "" && valorBruto !== null) {
            
            // 🚀 CORREÇÃO AQUI: Agora ele obedece ao 'tipo' definido no array
            const valorNormalizado = (alvo.tipo === "data") 
                                ? Sanitizador.dataPura(valorBruto) 
                                : Sanitizador.numeroPuro(valorBruto);
            
            range.setValue(valorNormalizado);       
            
            try {
              range.setNumberFormat(alvo.mascara);  
            } catch (eVisual) { }
          }
        }
      });
    }
};


// ============================================================================
// PONTO DE ENTRADA (Trigger Manual/Menu)
// ============================================================================

function atualizarNecton() { 
  PortfolioUpdater.syncPortfolioData(); 
}

// ============================================================================
// SUÍTE DE HOMOLOGAÇÃO
// ============================================================================

function testSuitePortfolio006() {
  console.log("=== INICIANDO HOMOLOGAÇÃO MOTOR 006 (v4.2.1) ===");
  const ticker = "PETRC425"; 
  const dados = PortfolioUpdater._fetchOptionData(ticker);
  if (dados) console.log("✅ Parser Puro OK:", dados);
}
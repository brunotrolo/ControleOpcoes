/**
 * @fileoverview Sanitizador Global - O Firewall de Dados
 * Responsável por garantir tipagem estrita no banco de dados.
 * Remove entropia (R$, %, espaços, formatos pt-BR) antes de gravar na célula.
 */

const Sanitizador = {
  
  /**
   * Converte "R$ 1.500,50", "15,4%" ou "0,15" em Float puro (1500.50, 0.154, 0.15).
   * À prova de falhas para o padrão brasileiro de corretoras.
   */
  numeroPuro(valor) {
    if (valor === null || valor === undefined || valor === '') return 0;
    if (typeof valor === 'number') return valor; // Já é puro

    let s = String(valor).trim().toUpperCase();

    // 1. Remove lixo visual
    s = s.replace(/[R$\s]/g, '');
    
    // Identifica se é percentual (para dividir por 100 no final)
    const isPercent = s.includes('%');
    s = s.replace('%', '');

    // 2. Trata a vírgula e o ponto (A Torre de Babel)
    if (s.includes(',') && s.includes('.')) {
      // Padrão BR completo (ex: 1.500,50) -> Ponto é milhar, Vírgula é decimal
      s = s.replace(/\./g, '').replace(',', '.');
    } else if (s.includes(',')) {
      // Só tem vírgula (ex: 0,15 ou 1500,50) -> É decimal
      s = s.replace(',', '.');
    } else if (s.includes('.')) {
      // Só tem ponto. Pode ser decimal US (1500.50) ou milhar BR s/ decimais (1.500)
      const partes = s.split('.');
      // Se a última parte tiver exatamente 3 dígitos, assumimos que é milhar
      if (partes.length > 1 && partes[partes.length - 1].length === 3) {
        s = s.replace(/\./g, ''); // Arranca o milhar
      }
    }

    let num = parseFloat(s);
    if (isNaN(num)) num = 0;

    return isPercent ? num / 100 : num;
  },

  /**
   * Remove espaços duplos e garante maiúsculas. Protege os Tickers.
   */
  textoPuro(valor) {
    if (valor === null || valor === undefined) return "";
    return String(valor).trim().toUpperCase();
  },

  /**
   * Força a criação de um Objeto Date nativo, independente de como a corretora mandou.
   */
  dataPura(valor) {
    if (!valor) return "";
    if (valor instanceof Date) return valor; // Já é puro

    let s = String(valor).trim();
    
    // Tratamento de padrão BR (DD/MM/YYYY)
    if (s.includes('/')) {
      let partes = s.split(' ')[0].split('/'); // Ignora a hora por enquanto
      if (partes.length === 3) {
        let dia = parseInt(partes[0], 10);
        let mes = parseInt(partes[1], 10) - 1; // JS usa mês 0-11
        let ano = parseInt(partes[2], 10);
        
        if (ano < 100) ano += 2000; // Trata '26' como '2026'
        return new Date(ano, mes, dia); // Retorna objeto nativo!
      }
    }
    
    // Fallback para ISO
    const d = new Date(valor);
    return isNaN(d.getTime()) ? "" : d;
  }
};


/**
 * 🧹 ASPIRADOR RETROATIVO V2 (Inteligente)
 * Varre toda a aba NECTON_IMPORT e lava números e datas com ferramentas específicas.
 */
function limparPassadoNecton() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const aba = ss.getSheetByName("NECTON_IMPORT"); 
  if (!aba) {
    SpreadsheetApp.getUi().alert("Aba NECTON_IMPORT não encontrada!");
    return;
  }

  const maxRows = aba.getLastRow();
  if (maxRows < 2) return;

  const headers = aba.getRange(1, 1, 1, aba.getLastColumn()).getValues()[0];
  const colMap = {};
  headers.forEach((label, index) => {
    if (label) colMap[String(label).trim().toUpperCase()] = index + 1;
  });

  // 🚀 AGORA COM TIPAGEM: Define se o alvo é 'numero' ou 'data'
  const colunasAlvo = [
    { nome: "ENTRY_PRICE", tipo: "numero", mascara: '"R$ "#,##0.00' },
    { nome: "LAST_PREMIUM", tipo: "numero", mascara: '"R$ "#,##0.00' },
    { nome: "LIMIT_PRICE", tipo: "numero", mascara: '"R$ "#,##0.00' },
    { nome: "STRIKE", tipo: "numero", mascara: '"R$ "#,##0.00' },
    { nome: "QUANTITY", tipo: "numero", mascara: '#,##0' },
    { nome: "ORDER_DATE", tipo: "data", mascara: 'dd/MM/yyyy HH:mm:ss' },
    { nome: "EXPIRY", tipo: "data", mascara: 'dd/MM/yyyy' }
  ];

  let celulasLavadas = 0;

  for (let linha = 2; linha <= maxRows; linha++) {
    colunasAlvo.forEach(alvo => {
      const colIndex = colMap[alvo.nome];
      if (colIndex) {
        const range = aba.getRange(linha, colIndex);
        
        if (range.getFormula() !== "") return;

        const valorBruto = range.getValue();
        
        if (valorBruto !== "" && valorBruto !== null) {
          
          // O CÉREBRO: Se for data, usa a lavanderia de datas. Senão, de números.
          const valorNormalizado = (alvo.tipo === "data") 
                              ? Sanitizador.dataPura(valorBruto) 
                              : Sanitizador.numeroPuro(valorBruto);
          
          range.setValue(valorNormalizado);
          try {
            range.setNumberFormat(alvo.mascara);
          } catch (eVisual) { }
          
          celulasLavadas++;
        }
      }
    });
  }

  SpreadsheetApp.getUi().alert(`✅ Limpeza Concluída!\nForam lavadas e convertidas ${celulasLavadas} células.`);
}



/**
 * 🧹 ASPIRADOR RETROATIVO EM LOTE: DADOS_DETALHES (Alta Performance)
 * Engole a base toda pra RAM, lava no Javascript e devolve de uma vez. Foge do limite de 6 minutos.
 */
function limparPassadoDetalhes() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const aba = ss.getSheetByName("DADOS_DETALHES"); 
  if (!aba) {
    SpreadsheetApp.getUi().alert("Aba DADOS_DETALHES não encontrada!");
    return;
  }

  const maxRows = aba.getLastRow();
  const maxCols = aba.getLastColumn();
  if (maxRows < 2) return;

  // 1. MAPEAMENTO DE CABEÇALHOS (Índice de Array começa em 0)
  const headers = aba.getRange(1, 1, 1, maxCols).getValues()[0];
  const colMap = {};
  headers.forEach((label, index) => {
    if (label) colMap[String(label).trim().toUpperCase()] = index; 
  });

  // 🚀 2. O GRANDE SAQUE (Engole tudo pra memória)
  const bodyRange = aba.getRange(2, 1, maxRows - 1, maxCols);
  const data = bodyRange.getValues();
  const formulas = bodyRange.getFormulas(); // Traz as fórmulas também para sabermos onde NÃO pisar

  const colunasAlvo = [
    { nome: "OPEN", tipo: "numero" },
    { nome: "HIGH", tipo: "numero" },
    { nome: "LOW", tipo: "numero" },
    { nome: "CLOSE", tipo: "numero" },
    { nome: "BID", tipo: "numero" },
    { nome: "ASK", tipo: "numero" },
    { nome: "STRIKE", tipo: "numero" },
    { nome: "SPOT", tipo: "numero" },
    { nome: "STRIKE_EOD", tipo: "numero" },
    { nome: "VARIATION", tipo: "numero" },
    { nome: "VOLUME_QTY", tipo: "numero" },
    { nome: "VOLUME_FIN", tipo: "numero" },
    { nome: "TRADES_COUNT", tipo: "numero" },
    { nome: "LOT_SIZE", tipo: "numero" },
    { nome: "BID_VOLUME", tipo: "numero" },
    { nome: "ASK_VOLUME", tipo: "numero" },
    { nome: "DTE_CALENDAR", tipo: "numero" },
    { nome: "UPDATED_AT", tipo: "data" },
    { nome: "CREATED_AT", tipo: "data" },
    { nome: "EDITED_AT", tipo: "data" },
    { nome: "DIVIDEND_UPDATED_AT", tipo: "data" },
    { nome: "EXPIRY", tipo: "vencimento" },
    { nome: "EXCH_TIMESTAMP", tipo: "epoch" },
    { nome: "LAST_TRADE_AT", tipo: "epoch" }
  ];

  let celulasLavadas = 0;

  // 3. LAVANDERIA NA MEMÓRIA RAM (Na velocidade da luz)
  for (let r = 0; r < data.length; r++) {
    colunasAlvo.forEach(alvo => {
      const colIndex = colMap[alvo.nome];
      
      if (colIndex !== undefined) {
        // 🛡️ Proteção de Fórmulas
        if (formulas[r][colIndex] !== "") return;

        const valorBruto = data[r][colIndex];
        
        if (valorBruto !== "" && valorBruto !== null && valorBruto !== "null") {
          let valorNormalizado = valorBruto;

          // 🧠 O CÉREBRO TRADUTOR
          if (alvo.tipo === "numero") {
            valorNormalizado = Sanitizador.numeroPuro(valorBruto);
          } 
          else if (alvo.tipo === "data") {
            valorNormalizado = Sanitizador.dataPura(valorBruto);
          } 
          else if (alvo.tipo === "vencimento") {
            let d = Sanitizador.dataPura(valorBruto);
            if (d instanceof Date) d.setHours(0, 0, 0, 0); 
            valorNormalizado = d;
          } 
          else if (alvo.tipo === "epoch") {
              let num = Number(val);
              if (typeof num === 'number' && num < 1577836800000) {
                  val = ""; 
              } else {
                  val = new Date(num);
              }
          }

          data[r][colIndex] = valorNormalizado;
          celulasLavadas++;
        }
      }
    });
  }

  // 🚀 4. A GRANDE DEVOLUÇÃO (Cospe tudo de uma vez no Sheets)
  bodyRange.setValues(data);

  SpreadsheetApp.getUi().alert(`✅ Limpeza em Lote Concluída!\nO tempo caiu de minutos para segundos.\nForam processadas ${celulasLavadas} células em alta velocidade.`);
}


/**
 * 🧹 ASPIRADOR RETROATIVO EM LOTE: DADOS_GREEKS (Alta Performance)
 * Varre a aba do Motor de Risco, expurga textos como "R$" e formata 
 * Gregas e Datas em segundos, respeitando a localidade americana.
 */
function limparPassadoGregas() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const aba = ss.getSheetByName("DADOS_GREEKS"); // Nome exato da sua aba
  if (!aba) {
    SpreadsheetApp.getUi().alert("Aba DADOS_GREEKS não encontrada!");
    return;
  }

  const maxRows = aba.getLastRow();
  const maxCols = aba.getLastColumn();
  if (maxRows < 2) return;

  // 1. MAPEAMENTO DE CABEÇALHOS (Índice de Array 0-based)
  const headers = aba.getRange(1, 1, 1, maxCols).getValues()[0];
  const colMap = {};
  headers.forEach((label, index) => {
    if (label) colMap[String(label).trim().toUpperCase()] = index; 
  });

  // 🚀 2. O GRANDE SAQUE (Engole tudo pra memória RAM)
  const bodyRange = aba.getRange(2, 1, maxRows - 1, maxCols);
  const data = bodyRange.getValues();
  const formulas = bodyRange.getFormulas(); 

  // 🎯 O MAPA TÁTICO DO MOTOR DE RISCO
  const colunasAlvo = [
    // Preços e Matemática Pura
    { nome: "PRICE", tipo: "numero" },
    { nome: "SPOT", tipo: "numero" },
    { nome: "STRIKE", tipo: "numero" },
    { nome: "MARGIN", tipo: "numero" },
    { nome: "MONEYNESS_RATIO", tipo: "numero" },
    { nome: "POE", tipo: "numero" },
    { nome: "IV_CALC", tipo: "numero" },

    // Gregas Direcionais (Sensibilidade alta)
    { nome: "DELTA", tipo: "numero" },
    { nome: "GAMMA", tipo: "numero" },
    { nome: "VEGA", tipo: "numero" },
    { nome: "THETA", tipo: "numero" },
    { nome: "RHO", tipo: "numero" },

    // Datas ISO (Timestamp do cálculo)
    { nome: "UPDATED_AT", tipo: "data" }
  ];

  let celulasLavadas = 0;

  // 3. LAVANDERIA NA MEMÓRIA RAM
  for (let r = 0; r < data.length; r++) {
    colunasAlvo.forEach(alvo => {
      const colIndex = colMap[alvo.nome];
      
      if (colIndex !== undefined) {
        // 🛡️ Proteção: Se houver alguma fórmula (ex: Custo Teórico), ele pula
        if (formulas[r][colIndex] !== "") return;

        const valorBruto = data[r][colIndex];
        
        if (valorBruto !== "" && valorBruto !== null && valorBruto !== "null") {
          let valorNormalizado = valorBruto;

          // O CÉREBRO TRADUTOR: Remove os "R$" perdidos e limpa a data
          if (alvo.tipo === "numero") {
            valorNormalizado = Sanitizador.numeroPuro(valorBruto);
          } else if (alvo.tipo === "data") {
            valorNormalizado = Sanitizador.dataPura(valorBruto);
          }

          // Salva o dado purificado na matriz
          data[r][colIndex] = valorNormalizado;
          celulasLavadas++;
        }
      }
    });
  }

  // 🚀 4. A GRANDE DEVOLUÇÃO 
  bodyRange.setValues(data);

  SpreadsheetApp.getUi().alert(`✅ Limpeza do Motor de Risco (Gregas) Concluída!\nForam purificadas ${celulasLavadas} células na velocidade da luz.`);
}
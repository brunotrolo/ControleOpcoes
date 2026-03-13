/**
 * ============================================================================
 * API.gs - The "State of the Art" Universal Backend Repository (Refatorado)
 * FOCO ATUAL: 10 Abas Core (Ingestão, Base, Cálculo e Interface)
 * ============================================================================
 */

// ==========================================
// 1. READ (Leitura Universal de Todo o Banco)
// ==========================================

/**
 * 🚀 FASE 1: VOO RÁPIDO
 * Carrega apenas o essencial para a tela acender imediatamente.
 *
 * 🔧 FIX: Strings literais substituídas por SYS_CONFIG.SHEETS.*
 *
 * ⚠️ ATENÇÃO: Mantido getDisplayValues() intencionalmente.
 *    O GAS não serializa Date objects via google.script.run — getValues() retorna
 *    Dates nativos que quebram a comunicação com o frontend com response: null.
 *    A conversão de tipos fica exclusivamente no Tradutor.html.
 */
function getDadosLight() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const abasEssenciais = [
      SYS_CONFIG.SHEETS.COCKPIT,
      SYS_CONFIG.SHEETS.CONFIG
    ];

    const data = {
      success: true,
      timestamp: new Date().toLocaleString('pt-BR'),
      raw: {}
    };

    abasEssenciais.forEach(nomeAba => {
      const sheet = getPlanilhaDinamica(ss, nomeAba);
      if (sheet) {
        const lastRow = sheet.getLastRow();
        data.raw[sheet.getName()] = lastRow === 0 ? [] : sheet.getDataRange().getDisplayValues();
      }
    });

    return data;
  } catch (e) {
    return { success: false, error: e.toString() };
  }
}

/**
 * 🚚 FASE 2: CARGA PESADA (Background)
 * Otimizado APENAS para as 10 abas do Core de Operações atuais.
 * Séries legadas (400, 500, 600) foram expurgadas para economizar memória e tempo de requisição.
 *
 * 🔧 FIX: Strings literais substituídas por SYS_CONFIG.SHEETS.*
 * ⚠️ ATENÇÃO: Mantido getDisplayValues() intencionalmente — mesma razão do getDadosLight.
 */
function getAbasPesadas() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const abasPesadas = [
      // 1. Ingestão & Logs
      SYS_CONFIG.SHEETS.IMPORT,
      SYS_CONFIG.SHEETS.LOGS,

      // 2. Base de Dados & Histórico
      SYS_CONFIG.SHEETS.ASSETS,
      SYS_CONFIG.SHEETS.HIST_250D,
      SYS_CONFIG.SHEETS.DETAILS,

      // 3. Motor Matemático & Filtros
      SYS_CONFIG.SHEETS.GREEKS_CALC,
      SYS_CONFIG.SHEETS.GREEKS_API,
      SYS_CONFIG.SHEETS.SELECTION_OPT
    ];

    const data = { success: true, timestamp: new Date().toLocaleString('pt-BR'), raw: {} };

    abasPesadas.forEach(nomeAba => {
      const sheet = getPlanilhaDinamica(ss, nomeAba);
      if (sheet) {
        const lastRow = sheet.getLastRow();
        data.raw[sheet.getName()] = lastRow === 0 ? [] : sheet.getDataRange().getDisplayValues();
      }
    });

    return data;
  } catch (e) {
    return { success: false, error: e.toString() };
  }
}

// ==========================================
// FUNÇÕES AUXILIARES (Core de Busca)
// ==========================================

/**
 * 🛡️ BUSCA DE PLANILHA DINÂMICA
 * Encontra a aba independentemente de Case Sensitive (maiúsculas/minúsculas).
 */
function getPlanilhaDinamica(planilhaAtiva, nomeProcurado) {
  const abas = planilhaAtiva.getSheets();
  const nomeProcuradoUpper = String(nomeProcurado).toUpperCase();

  // Otimização: For loop tradicional é levemente mais rápido que .find() no V8 do Apps Script
  for (let i = 0; i < abas.length; i++) {
    if (abas[i].getName().toUpperCase() === nomeProcuradoUpper) {
      return abas[i];
    }
  }
  return null;
}

function getAbaDinamica(payloadRaw, nomeProcurado) {
  const nomeProcuradoUpper = String(nomeProcurado).toUpperCase();
  const chaves = Object.keys(payloadRaw);

  for (let i = 0; i < chaves.length; i++) {
    if (String(chaves[i]).toUpperCase() === nomeProcuradoUpper) {
      return payloadRaw[chaves[i]];
    }
  }
  return null;
}

// ==========================================
// 🧪 MÓDULO DE TESTE (Para Homologação)
// ==========================================

/**
 * Rode esta função diretamente no Google Apps Script para validar
 * se o servidor consegue ler as 10 abas perfeitamente.
 */
function testarAPI_Leitura() {
  Logger.log("Iniciando Teste: getDadosLight()...");
  const light = getDadosLight();
  Logger.log("Status Light: " + light.success);
  Logger.log("Abas carregadas no Light: " + Object.keys(light.raw).join(", "));

  Logger.log("-----------------------------------------");

  Logger.log("Iniciando Teste: getAbasPesadas()...");
  const pesadas = getAbasPesadas();
  Logger.log("Status Pesadas: " + pesadas.success);
  Logger.log("Abas carregadas no Pesadas: " + Object.keys(pesadas.raw).join(", "));

  if (pesadas.error) {
    Logger.log("ERRO ENCONTRADO: " + pesadas.error);
  } else {
    Logger.log("✅ PARTE 1 HOMOLOGADA COM SUCESSO. Nenhuma falha de leitura.");
  }
}


// ==========================================
// 2. CREATE (Inserção em Lote)
// ==========================================

/**
 * 🔧 FIX: Lógica de busca de linha vazia substituída por getLastRow() + 1.
 *         A abordagem anterior era frágil — qualquer célula vazia no meio dos
 *         dados causava inserção na posição errada.
 */
function apiAdicionarLinhas(nomeAba, dadosMatriz) {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(nomeAba);
    if (!sheet) throw new Error(`Aba [${nomeAba}] não existe no banco de dados.`);
    if (!dadosMatriz || dadosMatriz.length === 0) return { success: true, message: "Nenhum dado para inserir." };

    const startRow = sheet.getLastRow() + 1;

    sheet.getRange(startRow, 1, dadosMatriz.length, dadosMatriz[0].length).setValues(dadosMatriz);
    SpreadsheetApp.flush(); // 🔒 Trava de segurança: Força a gravação física imediata

    return { success: true, message: `${dadosMatriz.length} linhas adicionadas em [${nomeAba}].` };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ==========================================
// 3. UPDATE (Atualização de Chave-Valor)
// ==========================================

function apiAtualizarChaveValor(nomeAba, payload) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = getPlanilhaDinamica(ss, nomeAba);
    if (!sheet) throw new Error(`Aba de configurações [${nomeAba}] não encontrada.`);

    const data = sheet.getDataRange().getValues();
    const chavesNovas = Object.keys(payload);
    let atualizacoes = 0;

    // Percorre a planilha procurando as chaves enviadas
    for (let i = 0; i < data.length; i++) {
      const chavePlanilha = String(data[i][0]).trim();
      if (chavesNovas.includes(chavePlanilha)) {
        sheet.getRange(i + 1, 2).setValue(payload[chavePlanilha]);
        atualizacoes++;
      }
    }

    SpreadsheetApp.flush();
    return { success: true, message: `${atualizacoes} chaves atualizadas com sucesso.` };
  } catch (e) {
    return { success: false, error: e.toString() };
  }
}

function apiSetCellValue(nomeAba, linha, coluna, valor) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(nomeAba);
    if (!sheet) throw new Error(`Aba [${nomeAba}] não encontrada.`);

    // Operação atômica
    sheet.getRange(linha, coluna).setValue(valor);
    return { success: true, timestamp: new Date().toLocaleTimeString() };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ==========================================
// 4. DELETE & TRUNCATE (Exclusão e Limpeza)
// ==========================================

function apiExcluirLinhaSegura(nomeAba, numeroLinha, valorEsperadoColunaA) {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(nomeAba);
    if (!sheet) throw new Error(`Aba [${nomeAba}] não existe no banco de dados.`);

    const valorPlanilha = String(sheet.getRange(numeroLinha, 1).getDisplayValue() || "").trim().toUpperCase();
    const valorSeguro = String(valorEsperadoColunaA || "").trim().toUpperCase();

    // Trava anti-dessincronização (Double Check)
    if (valorPlanilha !== valorSeguro) {
      return { success: false, error: `Falha de sincronia: Esperava encontrar [${valorSeguro}], mas encontrou [${valorPlanilha}] na linha ${numeroLinha}. Exclusão abortada.` };
    }

    sheet.deleteRow(numeroLinha);
    SpreadsheetApp.flush();
    return { success: true, message: `Registro [${valorSeguro}] removido de [${nomeAba}].` };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function apiExcluirLinhasEmLote(nomeAba, listaLinhas) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(nomeAba);
    if (!sheet) throw new Error("Aba não encontrada: " + nomeAba);

    // FILTRO DE SEGURANÇA: Remove nulos, converte para inteiro e ordena de baixo para cima
    const linhasOrdenadas = listaLinhas
      .filter(l => l !== null && l !== undefined && !isNaN(l))
      .map(l => parseInt(l, 10))
      .filter(l => l > 0) // Impede deleção de linha 0 ou negativa
      .sort((a, b) => b - a); // ⚠️ OBRIGATÓRIO: Deletar de baixo para cima para não mudar o índice das linhas de cima

    linhasOrdenadas.forEach(linha => {
      sheet.deleteRow(linha);
    });

    SpreadsheetApp.flush();
    return { success: true, count: linhasOrdenadas.length };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function apiLimparAba(nomeAba, manterLinhasTop = 1, mensagemAuditoria = null) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(nomeAba);
    if (!sheet) throw new Error(`Aba [${nomeAba}] não existe no banco de dados.`);

    const lastRow = sheet.getLastRow();
    if (lastRow > manterLinhasTop) {
      sheet.getRange(manterLinhasTop + 1, 1, lastRow - manterLinhasTop, sheet.getLastColumn()).clearContent();
    }

    // Rastro de auditoria
    if (mensagemAuditoria) {
      const ts = Utilities.formatDate(new Date(), ss.getSpreadsheetTimeZone(), "dd/MM/yyyy HH:mm:ss");
      sheet.getRange(manterLinhasTop + 1, 1, 1, 4).setValues([[ts, "SYSTEM", "AVISO", mensagemAuditoria]]);
    }

    SpreadsheetApp.flush();
    return { success: true, message: `Aba [${nomeAba}] foi limpa, mantendo ${manterLinhasTop} linha(s) de cabeçalho.` };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ==========================================
// 🧪 MÓDULO DE TESTE DA PARTE 2 (Homologação)
// ==========================================

function testarAPI_Escrita() {
  Logger.log("Iniciando Teste de Escrita na aba [Logs]...");

  // 1. Testa Inserção (Adiciona um Log falso)
  const timestamp = new Date().toLocaleString();
  const resInsert = apiAdicionarLinhas(SYS_CONFIG.SHEETS.LOGS, [[timestamp, "SISTEMA_TESTE", "INFO", "Teste de Homologação da API de Escrita", ""]]);
  Logger.log("Adicionar Linha: " + resInsert.success + " | " + resInsert.message);

  // 2. Testa Limpeza Segura (Limpa os Logs mantendo o cabeçalho e adicionando auditoria)
  const resLimpar = apiLimparAba(SYS_CONFIG.SHEETS.LOGS, 1, "Auditoria de teste gerada pelo testarAPI_Escrita.");
  Logger.log("Limpar Aba Segura: " + resLimpar.success + " | " + resLimpar.message);

  if (resInsert.error || resLimpar.error) {
    Logger.log("❌ ERRO ENCONTRADO DURANTE A ESCRITA/LIMPEZA.");
  } else {
    Logger.log("✅ PARTE 2 HOMOLOGADA COM SUCESSO. Banco de dados seguro.");
  }
}


// ==========================================
// 5. EXTERNAL API BRIDGE (Integrações de Terceiros)
// ==========================================

function apiIntegracaoOpLab(ticker) {
  if (!ticker || String(ticker).trim() === '') {
    return { success: false, error: 'Ticker não fornecido.' };
  }

  try {
    const cleanTicker = String(ticker).toUpperCase().trim();
    const data = OplabService.getOptionDetails(cleanTicker);

    if (!data) return { success: false, error: 'Ativo [' + cleanTicker + '] não encontrado ou sem liquidez.' };

    return {
      success: true,
      data: {
        symbol:      data.symbol      || cleanTicker,
        category:    data.category    || 'N/A',
        strike:      parseFloat(data.strike            || 0),
        premioAtual: parseFloat(data.close > 0 ? data.close : (data.bid || 0)),
        spotPrice:   parseFloat(data.spot_price        || 0),
        dte:         parseInt(data.days_to_maturity    || 0)
      }
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ==========================================
// 6. ORQUESTRAÇÃO DE ESTADO E SIMULAÇÃO
// ==========================================

/**
 * Atualiza o horizonte na Config_Global e tenta rodar o pipeline.
 * Preparado para degradação graciosa (se o pipeline for deletado, ele não quebra).
 *
 * 🔧 FIX: String literal "Config_Global" substituída por SYS_CONFIG.SHEETS.CONFIG
 */
function apiSimularHorizontePreditivo(diasParam) {
  try {
    const dias = parseInt(diasParam, 10);
    if (isNaN(dias) || dias < 1 || dias > 45) {
      throw new Error("Horizonte inválido. O parâmetro deve ser um número entre 1 e 45.");
    }

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const abaConfig = getPlanilhaDinamica(ss, SYS_CONFIG.SHEETS.CONFIG);
    if (!abaConfig) throw new Error(`Aba [${SYS_CONFIG.SHEETS.CONFIG}] não encontrada no banco de dados.`);

    const dados = abaConfig.getDataRange().getValues();
    let configuracaoAtualizada = false;

    for (let i = 0; i < dados.length; i++) {
      if (String(dados[i][0]).trim() === "Regra_Dias_Horizonte_Preditivo") {
        abaConfig.getRange(i + 1, 2).setValue(dias);
        configuracaoAtualizada = true;
        break;
      }
    }

    if (!configuracaoAtualizada) {
      abaConfig.appendRow([
        "Regra_Dias_Horizonte_Preditivo",
        dias,
        "[SISTEMA] Horizonte de simulação configurado via Web App"
      ]);
    }

    SpreadsheetApp.flush();

    // Tenta acionar o recálculo, mas não quebra se os arquivos legados não existirem
    let statusPipeline = "Salvo em Config_Global (Modo Standby).";
    if (typeof executarFluxoSequencial === "function") {
      executarFluxoSequencial();
      statusPipeline = "Pipeline sequencial acionado com sucesso.";
    } else if (typeof gerarAnalisePreditivaHeatmap === "function") {
      gerarAnalisePreditivaHeatmap(dias);
      statusPipeline = "Heatmap preditivo atualizado isoladamente.";
    }

    return {
      success: true,
      mensagem: `Simulação para ${dias} dias processada. ${statusPipeline}`,
      horizonte: dias
    };

  } catch (error) {
    return { success: false, error: error.message };
  }
}

// ==========================================
// 🧪 MÓDULO DE TESTE DA PARTE 3 (Homologação)
// ==========================================

function testarAPI_Integracoes() {
  Logger.log("Iniciando Teste da Parte 3...");

  // Teste de Estado
  const resSimulador = apiSimularHorizontePreditivo(15);
  Logger.log("Atualização de Configuração: " + resSimulador.success + " | " + resSimulador.mensagem);

  if (resSimulador.error) {
    Logger.log("❌ ERRO ENCONTRADO NA PARTE 3.");
  } else {
    Logger.log("✅ PARTE 3 HOMOLOGADA COM SUCESSO. Arquivo API.gs finalizado!");
  }
}
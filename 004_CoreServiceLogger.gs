/**
 * @fileoverview SysLogger & DataExtractor - v3.1
 * RESPONSABILIDADE: Rastreabilidade total via Buffer e extração de dados locais.
 * INTEGRAÇÃO: Utiliza SYS_CONFIG (001) para caminhos e DataUtils (002) para parsing.
 */

// ============================================================================
// MOTOR DE LOGS (SysLogger)
// ============================================================================

const SysLogger = {
  _buffer: [],
  _levels: { INFO: "INFO", SUCESSO: "SUCESSO", AVISO: "AVISO", ERRO: "ERRO", CRITICO: "CRITICO" },

  /**
   * Registra um evento no buffer de memória (Custo: 0ms).
   */
  log(servico, nivel, mensagem, contexto = "") {
    try {
      const timestamp = new Date();
      let ctxFormatado = "";

      // Tratamento de contexto (Erro, Objeto ou String)
      if (contexto instanceof Error) {
        ctxFormatado = `Msg: ${contexto.message}\nStack: ${contexto.stack}`;
      } else if (typeof contexto === 'object') {
        try { ctxFormatado = JSON.stringify(contexto, null, 2); } catch (e) { ctxFormatado = String(contexto); }
      } else {
        ctxFormatado = String(contexto);
      }

      // Trava de segurança contra limite de célula do Sheets (50k chars)
      if (ctxFormatado.length > 45000) ctxFormatado = ctxFormatado.substring(0, 45000) + "\n...[TRUNCADO]";

      this._buffer.push([timestamp, servico || "SISTEMA", nivel || "INFO", mensagem || "", ctxFormatado]);

      // Erros críticos forçam gravação imediata
      if (nivel === "CRITICO") this.flush();

    } catch (e) { console.error(`[Logger_Panic]: ${e.message}`); }
  },

  /**
   * Descarrega o buffer na aba de Logs em Batch (Escrita única).
   */
  flush() {
    if (this._buffer.length === 0) return;
    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const abaNome = SYS_CONFIG.SHEETS.LOGS;
      const sheet = ss.getSheetByName(abaNome);
      
      if (!sheet) {
        console.warn(`[SysLogger] Aba '${abaNome}' não encontrada. Criando logs no console.`);
        console.info(this._buffer);
        return;
      }

      sheet.getRange(sheet.getLastRow() + 1, 1, this._buffer.length, 5).setValues(this._buffer);
      this._buffer = []; // Limpa memória
      SpreadsheetApp.flush(); 
    } catch (e) { console.error(`[Flush_Panic]: ${e.message}`); }
  }
};

// ============================================================================
// SERVIÇO DE EXTRAÇÃO (DataExtractor)
// ============================================================================

const DataExtractorService = {
  
  /**
   * Extrai dados ativos do Cockpit transformando linhas em Objetos.
   */
  extractActiveCockpit() {
    SysLogger.log("Extractor", "INFO", "Iniciando extração do Cockpit");
    
    try {
      const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SYS_CONFIG.SHEETS.COCKPIT);
      if (!sheet) throw new Error("Aba Cockpit não encontrada");

      const LINHA_CABECALHO = 10;
      const lastRow = sheet.getLastRow();
      if (lastRow <= LINHA_CABECALHO) return [];

      const data = sheet.getRange(LINHA_CABECALHO, 1, lastRow - (LINHA_CABECALHO - 1), sheet.getLastColumn()).getValues();
      const headers = data[0].map(h => String(h).trim());
      const rows = data.slice(1);

      // Filtra apenas o que está "ATIVO" e converte para Objeto
      const statusIdx = headers.indexOf("STATUS");
      
      const result = rows
        .filter(r => String(r[statusIdx]).toUpperCase() === "ATIVO")
        .map(r => {
          let obj = {};
          headers.forEach((h, i) => { if(h) obj[h] = r[i]; });
          return obj;
        });

      SysLogger.log("Extractor", "SUCESSO", `Carregados ${result.length} ativos.`);
      return result;
    } catch (e) {
      SysLogger.log("Extractor", "CRITICO", "Falha na extração", e);
      return [];
    }
  }
};

// ============================================================================
// SUÍTE DE TESTES (Homologação 003)
// ============================================================================

function testSuiteLoggerV3() {
  console.log("=== HOMOLOGANDO LOGGER v3.1 ===");
  
  // 1. Teste de Registro
  SysLogger.log("Teste", "INFO", "Log de teste unitário", { status: "OK" });
  console.log(`Buffer contém: ${SysLogger._buffer.length} itens. ${SysLogger._buffer.length === 1 ? "✅" : "❌"}`);

  // 2. Teste de Flush
  SysLogger.flush();
  console.log(`Buffer após flush: ${SysLogger._buffer.length} itens. ${SysLogger._buffer.length === 0 ? "✅" : "❌"}`);

  console.log("=== FIM DA HOMOLOGAÇÃO 003 ===");
}
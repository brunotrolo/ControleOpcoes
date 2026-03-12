/**
 * @fileoverview CoreDataUtils - v2.0 (The Swiss Army Knife)
 * RESPONSABILIDADE: Centralizar toda a inteligência de conversão e mapeamento.
 */

const DataUtils = {
  
  /** Converte BRL/String para Float Puro */
  // DEPOIS (002_CoreDataUtils.gs)
  /** 
   * @deprecated Use Sanitizador.numeroPuro() para dados de corretora/API.
   * Mantido apenas para compatibilidade com ConfigManager (001).
   */
  safeFloat(val) {
      if (val === undefined || val === null || val === "") return 0;
      if (typeof val === 'number') return val;
      let clean = val.toString().replace("R$", "").replace(/\s/g, "").replace(/\./g, "").replace(",", ".");
      let num = parseFloat(clean);
      return isNaN(num) ? 0 : num;
  },

  /** Padroniza Datas para o formato brasileiro DD/MM/YYYY */
  formatDateBR(raw) {
    if (!raw || raw === "N/A") return "N/A";
    try {
      const d = (raw instanceof Date) ? raw : new Date(String(raw).trim().split('T')[0].replace(/-/g, '/'));
      if (isNaN(d.getTime())) return String(raw);
      
      const dia = String(d.getDate()).padStart(2, '0');
      const mes = String(d.getMonth() + 1).padStart(2, '0');
      const ano = d.getFullYear();
      return `${dia}/${mes}/${ano}`;
    } catch (e) { return String(raw); }
  },

  /** Cria mapa de cabeçalhos { Nome: Indice_0 } */
  getHeaderMap(aba) {
    if (!aba) return {};
    const headers = aba.getRange(1, 1, 1, aba.getLastColumn()).getValues()[0];
    const map = {};
    headers.forEach((h, i) => { if(h) map[String(h).trim()] = i; });
    return map;
  },

  /** Mapeia ID para número da linha (Performance O(1)) */
  getRowMap(aba, colName = "ID_Trade_Unico") {
    const map = {};
    const lastRow = aba.getLastRow();
    if (lastRow < 2) return map;
    
    const headers = aba.getRange(1, 1, 1, aba.getLastColumn()).getValues()[0];
    const colIdx = headers.indexOf(colName);
    if (colIdx === -1) return map;

    const data = aba.getRange(2, colIdx + 1, lastRow - 1, 1).getValues();
    data.forEach((row, i) => { if (row[0]) map[String(row[0]).trim()] = i + 2; });
    return map;
  },

  /** 
   * Cria mapa de cabeçalhos { NOME_HEADER: indice_0based }
   * Substitui os _getColMap() duplicados nos motores 010, 011 e 012.
   */
  getColMap(aba) {
      if (!aba) return {};
      const headers = aba.getRange(1, 1, 1, aba.getLastColumn()).getValues()[0];
      const map = {};
      headers.forEach((h, i) => { if (h) map[String(h).trim().toUpperCase()] = i; });
      return map;
  },

  /**
   * Cria mapa { pkValue: rowObject } para lookup O(1) por chave primária.
   * Substitui os _getDynamicMap() duplicados nos motores 011 e 012.
   */
  getDynamicMap(aba, pkLabel) {
      if (!aba) return {};
      const data = aba.getDataRange().getValues();
      const headers = data[0];
      const pkIdx = headers.indexOf(pkLabel);
      if (pkIdx === -1) return {};
      const map = {};
      for (let i = 1; i < data.length; i++) {
          const obj = {};
          headers.forEach((h, idx) => { if (h) obj[String(h).trim()] = data[i][idx]; });
          if (data[i][pkIdx]) map[String(data[i][pkIdx]).trim()] = obj;
      }
      return map;
  },

  /** Lógica de Merge: Une dados novos aos existentes sem apagar colunas manuais */
  buildRowMerge(abaHeaders, existingRowData, newDataObj) {
    return abaHeaders.map((header, idx) => {
      const key = String(header).trim();
      if (newDataObj[key] !== undefined) return newDataObj[key];
      return existingRowData ? existingRowData[idx] : "";
    });
  }
};

// ============================================================================
// TESTES DE INTEGRAÇÃO DOS UTILITÁRIOS
// ============================================================================

function testSuiteDataUtilsV2() {
  console.log("=== TESTANDO UNIFICAÇÃO DATA UTILS v2.0 ===");
  
  // Teste Moeda
  const m = DataUtils.safeFloat("R$ 1.500,50");
  console.log(`[TEST] safeFloat: ${m === 1500.5 ? "✅" : "❌ ("+m+")"}`);

  // Teste Data (Vários formatos)
  const d1 = DataUtils.formatDateBR("2026-03-08");
  const d2 = DataUtils.formatDateBR(new Date(2026, 2, 8));
  console.log(`[TEST] Data ISO: ${d1 === "08/03/2026" ? "✅" : "❌ ("+d1+")"}`);
  console.log(`[TEST] Data Obj: ${d2 === "08/03/2026" ? "✅" : "❌ ("+d2+")"}`);

  console.log("=== FIM DOS TESTES ===");
}
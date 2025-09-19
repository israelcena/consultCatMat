const axios = require("axios");

class ImportCatmatCatserService {
  constructor() {
    // Comentado pois é específico do FaaS. Use console.log simulando banco.
    // this.catmatcatserDB = ...
    // this.catmatcatserLogDB = ...
    this.TIME_TO_SLEEP = 500;
    this.BASE_URL_CATMAT = "https://dadosabertos.compras.gov.br/modulo-material/4_consultarItemMaterial";
    this.BASE_URL_CATSER = "https://dadosabertos.compras.gov.br/modulo-servico/6_consultarItemServico";
  }
  async create(content) {
    await this.sleep(2000);
    console.log('Simula CREATE no banco:', content);
  }
  async update(content) {
    console.log('Simula UPDATE no banco:', content);
  }
  async findByIdcatmat(idcatmat) {
    await this.sleep(2000);
    // Simula busca: retorna null para sempre simular "não existe"
    // Ou retorne objetão fake se quiser testar update!
    console.log('Simula busca no banco catmat/catser pelo número:', idcatmat);
    return null;
    // Exemplo se quiser simular "já existe" para fazer update ao invés de create:
    // return { atualizacao: "2024-01-01T00:00:00.000Z" };
  }
  async logImportacao(content) {
    console.log('LOG registrador:', content);
  }
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
  mapItemCatmat(item) {
    return {
      idcatmat: item.codigoItem,
      nome: item.nomePdm,
      descricao: item.descricaoItem,
      tipo: "Permanente",
      codigo_universal_catmat: String(item.codigoItem),
      ativo: item.statusItem ? "S" : "N",
      atualizacao: item.dataHoraAtualizacao
    };
  }
  mapItemCatser(item) {
    return {
      idcatmat: item.codigoServico,
      nome: item.nomeServico,
      descricao: item.nomeClasse,
      tipo: "Serviço",
      codigo_universal_catmat: String(item.codigoServico),
      ativo: item.statusServico ? "S" : "N",
      atualizacao: item.dataHoraAtualizacao
    };
  }
  async importarService({ baseUrl, tipoLog, itemToRegistroTabela }) {
    let criados = 0, atualizados = 0, verificados = 0;
    try {
      // Descobrir total de páginas
      const primeiraUrl = `${baseUrl}?tamanhoPagina=500&pagina=1`;
      const primeiraResp = await axios.get(primeiraUrl);
      const totalPaginas = primeiraResp.data.totalPaginas;
      console.log(`${tipoLog} TOTAL DE PÁGINAS:`, totalPaginas);
      for (let pagina = 1; pagina <= totalPaginas; pagina++) {
        const url = `${baseUrl}?tamanhoPagina=500&pagina=${pagina}`;
        console.log(`${tipoLog} Baixando página ${pagina}/${totalPaginas}`);
        try {
          const resp = await axios.get(url);
          const itens = resp.data.resultado;
          for (const item of itens) {
            const registroTabela = itemToRegistroTabela(item);
            const anterior = await this.findByIdcatmat(registroTabela.idcatmat);
            if (!anterior) {
              await this.create(registroTabela);
              criados++;
            } else if (
              registroTabela.atualizacao && anterior.atualizacao === registroTabela.atualizacao
            ) {
              verificados++;
            } else {
              await this.update(registroTabela);
              atualizados++;
            }
          }
        } catch (errPag) {
          console.log(`${tipoLog} ERRO página ${pagina}:`, errPag.message);
          continue;
        }
        await this.sleep(this.TIME_TO_SLEEP);
      }
      console.log(`${tipoLog} -- FINALIZADO --`);
    } catch (err) {
      console.log(`${tipoLog} Erro inicial:`, err.message);
    }
    return { criados, atualizados, verificados };
  }
  static formatDurationVerbose(dateInicio, dateFim) {
    let ms = dateFim - dateInicio;
    if (typeof ms !== "number" || ms < 0) return "período inválido";
    const h = Math.floor(ms / 3600000);
    ms -= h * 3600000;
    const m = Math.floor(ms / 60000);
    ms -= m * 60000;
    const s = Math.floor(ms / 1000);
    const parts = [];
    if (h > 0) parts.push(`${h} hora${h > 1 ? 's' : ''}`);
    if (m > 0) parts.push(`${m} minuto${m > 1 ? 's' : ''}`);
    if (s > 0) parts.push(`${s} segundo${s > 1 ? 's' : ''}`);
    return parts.length > 0 ? parts.join(', ') : "menos de 1 segundo";
  }
  static gerarObservacaoLog(data_inicio, data_fim) {
    const duracaoTxt = this.formatDurationVerbose(data_inicio, data_fim);
    const dtString = data_fim.toLocaleString("pt-BR");
    return `A atualização foi gerada em ${dtString}, com ${duracaoTxt}.`;
  }
  async execute() {
    const data_inicio = new Date();
    const [catmatStats, catserStats] = await Promise.all([
    this.importarService({
      baseUrl: this.BASE_URL_CATMAT,
      tipoLog: '[CATMAT]',
      itemToRegistroTabela: this.mapItemCatmat,
    }),
    this.importarService({
      baseUrl: this.BASE_URL_CATSER,
      tipoLog: '[CATSER]',
      itemToRegistroTabela: this.mapItemCatser,
    })]);
    const data_fim = new Date();
    await this.logImportacao({
      origem: 'CATMAT/CATSER',
      total_criados: catmatStats.criados + catserStats.criados,
      total_atualizados: catmatStats.atualizados + catserStats.atualizados,
      total_verificados: catmatStats.verificados + catserStats.verificados,
      data_inicio,
      data_fim,
      observacao: ImportCatmatCatserService.gerarObservacaoLog(data_inicio, data_fim)
    });
    console.log("AMBOS PROCESSOS FINALIZADOS");
  }
}

const importCatmatCatserService = new ImportCatmatCatserService();
importCatmatCatserService.execute({});
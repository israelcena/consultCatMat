const axios = require("axios");

// Estrutura da tabela
// CREATE TABLE "lowcode_apps"."app_catmatser" (
// "id" SERIAL,
// "idcatmat" bigint NULL  << = codigoItem
// "nome" varchar(300) NOT NULL,  << = nomePdm
// "descricao" text NOT NULL,     << = descricaoItem
// "tipo" varchar(50) NULL ,      << = "Permanente"
// "codigo_universal_catmat" varchar(100) NULL , << = idcatmat (códigoItem), string!
// "ativo" bpchar(1) NULL ,       << = "S"
// )
const BASE_URL_CATMAT = "https://dadosabertos.compras.gov.br/modulo-material/4_consultarItemMaterial";
const BASE_URL_CATSER = "https://dadosabertos.compras.gov.br/modulo-servico/6_consultarItemServico";
const TIME_TO_SLEEP = 700;

// Função para criar registro no banco (aqui só exibe no console)
async function create(itemTabela) {
  // Substitua este console.log pelo seu insert real
  console.log("Vou criar no banco:", itemTabela);
}

async function update(itemTabela) {
  console.log("UPDATE:", itemTabela);
  // Atualize os campos do registro no banco com base no itemTabela.
}

async function findByIdcatmat(idcatmat) {
  // Buscar registro no banco onde idcatmat = idcatmat
  // Exemplo: SELECT * FROM tabela WHERE idcatmat = $1
  // Retorne null se não achou, ou objeto do registro se achou.
  return null; // Simula que não achou
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function importarService({
  baseUrl,
  tamanhoPagina = 500,
  tipoLog = '[GENÉRICO]',
  itemToRegistroTabela
}) {
  try {
    // Descobrir total de páginas
    const primeiraUrl = `${baseUrl}?tamanhoPagina=${tamanhoPagina}&pagina=1`;
    const primeiraResp = await axios.get(primeiraUrl);
    const totalPaginas = primeiraResp.data.totalPaginas;
    console.log(`${tipoLog} TOTAL DE PÁGINAS:`, totalPaginas);

    for (let pagina = 1; pagina <= totalPaginas; pagina++) {
      const url = `${baseUrl}?tamanhoPagina=${tamanhoPagina}&pagina=${pagina}`;
      console.log(`${tipoLog} Baixando página ${pagina}/${totalPaginas}`);
      try {
        const resp = await axios.get(url);
        const itens = resp.data.resultado;
        for (const item of itens) {
          const registroTabela = itemToRegistroTabela(item);
          const anterior = await findByIdcatmat(registroTabela.idcatmat);
          if (!anterior) {
            await create(registroTabela);
          } else if (
            registroTabela.atualizacao && anterior.atualizacao === registroTabela.atualizacao
          ) {
            // Não faz nada, está atualizado
          } else {
            await update(registroTabela);
          }
        }
      } catch (errPag) {
        console.error(`${tipoLog} ERRO página ${pagina}:`, errPag.message);
        continue;
      }
      await sleep(TIME_TO_SLEEP);
    }
    console.log(`${tipoLog} -- FINALIZADO --`);
  } catch (err) {
    console.error(`${tipoLog} Erro inicial:`, err.message);
  }
}

function mapItemCatmat(item) {
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

function mapItemCatser(item) {
  return {
    idcatmat: item.codigoServico,
    nome: item.nomeServico,
    descricao: item.nomeClasse,
    tipo: "Serviço",
    codigo_universal_catmat: String(item.codigoItem),
    ativo: item.statusServico ? "S" : "N",
    atualizacao: item.dataHoraAtualizacao
  };
}

async function importarCatmat() {
  await importarService({
    baseUrl: BASE_URL_CATMAT,
    tipoLog: '[CATMAT]',
    itemToRegistroTabela: mapItemCatmat
  });
}

async function importarCatser() {
  await importarService({
    baseUrl: BASE_URL_CATSER,
    tipoLog: '[CATSER]',
    itemToRegistroTabela: mapItemCatser
  });
}

// --- Função para rodar ambas paralelamente
async function importarCatmatECatserParalelo() {
  await Promise.all([
    importarCatmat(),
    importarCatser()
  ]);
  console.log("AMBOS PROCESSOS FINALIZADOS");
}

importarCatmatECatserParalelo();


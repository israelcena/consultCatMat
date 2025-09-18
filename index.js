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

const BASE_URL = "https://dadosabertos.compras.gov.br/modulo-material/4_consultarItemMaterial";
const TAMANHO_PAGINA = 500;
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

async function importarCatmat() {
  try {
    // Descobrir total de páginas
    const primeiraUrl = `${BASE_URL}?tamanhoPagina=${TAMANHO_PAGINA}&pagina=1`;
    const primeiraResp = await axios.get(primeiraUrl);
    const totalPaginas = primeiraResp.data.totalPaginas;
    console.log("TOTAL DE PÁGINAS:", totalPaginas);

    // Agora, para cada página...
    for (let pagina = 1; pagina <= totalPaginas; pagina++) {
      const url = `${BASE_URL}?tamanhoPagina=${TAMANHO_PAGINA}&pagina=${pagina}`;
      console.log(`Baixando página ${pagina}/${totalPaginas}`);

      try {
        const resp = await axios.get(url);
        const itens = resp.data.resultado;

        for (const item of itens) {
          // Monta objeto conforme tabela
          const registroTabela = {
            idcatmat: item.codigoItem,
            nome: item.nomePdm,
            descricao: item.descricaoItem,
            tipo: "Permanente",
            codigo_universal_catmat: String(item.codigoItem),
            ativo: item.statusItem ? "S" : "N",
            atualizacao: item.dataHoraAtualizacao
          };

          const existente = await findByIdcatmat(registroTabela.idcatmat);
          if (!existente) {
            await create(registroTabela);
          } else if (
            registroTabela.atualizacao &&
            anterior.atualizacao === registroTabela.atualizacao
          ) {
            // Não faz nada, está atualizado
          } else {
            await update(registroTabela);
          }
        }
      } catch (errPag) {
        console.error("ERRO página " + pagina + ":", errPag.message);
        continue;
        // Se quiser continuar mesmo com erro: continue;
        // Se quiser parar: throw errPag;
      }
      await sleep(TIME_TO_SLEEP);
    }
    console.log("-- FINALIZADO --");
  } catch (err) {
    console.error("Erro inicial:", err.message);
  }
}

async function importarCatser() {
  try {
    // Descobre total de páginas
    const primeiraUrl = `${SERVICE_URL}?tamanhoPagina=${TAMANHO_PAGINA}&pagina=1`;
    const primeiraResp = await axios.get(primeiraUrl);
    const totalPaginas = primeiraResp.data.totalPaginas;
    console.log("TOTAL DE PÁGINAS:", totalPaginas);

    // Para cada página...
    for (let pagina = 1; pagina <= totalPaginas; pagina++) {
      const url = `${SERVICE_URL}?tamanhoPagina=${TAMANHO_PAGINA}&pagina=${pagina}`;
      console.log(`Baixando página ${pagina}/${totalPaginas}`);

      try {
        const resp = await axios.get(url);
        const itens = resp.data.resultado;

        for (const item of itens) {
          // Monta objeto conforme tabela
          const registroTabela = {
            idcatmat: item.codigoServico,
            nome: item.nomeServico,
            descricao: item.nomeClasse,
            tipo: "Serviço",
            codigo_universal_catmat: String(item.codigoItem),
            ativo: item.statusServico ? "S" : "N",
            atualizacao: item.dataHoraAtualizacao
          };

          const existente = await findByIdcatmat(registroTabela.idcatmat);
          if (!existente) {
            await create(registroTabela);
          } else if (
            registroTabela.atualizacao &&
            anterior.atualizacao === registroTabela.atualizacao
          ) {
            // Não faz nada, está atualizado
          } else {
            await update(registroTabela);
          }

        }
      } catch (errPag) {
        console.error("ERRO página " + pagina + ":", errPag.message);
        continue; // Comente ou descomente conforme desejar pular a página com erro
      }
      await sleep(TIME_TO_SLEEP);
    }
    console.log("-- FINALIZADO [SERVIÇOS] --");
  } catch (err) {
    console.error("Erro inicial:", err.message);
  }
}

// --- Função para rodar ambas paralelamente
async function importarCatmatECatserParalelo() {
  await Promise.all([
    importarCatmat(),
    importarCatser()
  ]);
  console.log("AMBOS PROCESSOS FINALIZADOS");
}

// Para executar:
importarCatmatECatserParalelo();


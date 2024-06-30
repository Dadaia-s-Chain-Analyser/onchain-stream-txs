# Offchain Watchers Application

## 1 - Resumo

**Onchain-stream-txs** é um dos três componentes desenvolvidos, com a finalidade de interagir com redes blockchain públicas compatíveis com a [EVM (Ethereum Virtual Machine)](https://ethereum.org/en/developers/docs/evm/), por meio de software, e explorar as oportunidades oferecidas por esta tecnologia. Este pacote em particular, na sua versão final, é uma imagem Docker com aplicações escritas na linguagem Python. Tais aplicações trabalham de forma integrada se comunicando através de tópicos do Apache Kafka. Essa interação tem por objetivo obter dados de transações realizadas nessas redes de maneira otimizada e escalável, para satisfazer o requisito de processar esses dados em tempo real com a menor latência possível.

## 2 - Objetivo

O objetivo desse pacote é obter dados brutos de transações efetuadas em diferentes redes blockchain públicas, em tempo real com a finalidade de processá-los e ingestá-los em um tópico do kafka, tornando possível a integração com diferentes sistemas e possibilitando:

- Análise das transações, abrindo uma vasta gama de oportunidades de obtenção insights para operar nesses protocolos;
- Monitoramento de transações em tempo real;
- Uso dos dados em aplicações e bots de trading, flash loans e flash swaps;
- Uso dos dados para treinamento de modelos de machine learning.

## 3 - Introdução

O pacote **Onchain-stream-txs** é composto por aplicações escritas em python para obter dados brutos das transações efetuadas em redes blockchain EVM em tempo real. Um exemplo de [transação na rede principal Ethereum pode ser vista aqui](https://etherscan.io/tx/0x2d0f5e509165c64c755e1126e2b3671d80341f7f7f0a5b202a96a439839f69be).

### 3.1 - Como acessar dados de uma rede blockchain de forma escalável

A arquitetura desenvolvida para essa solução foi construída com intuito de otimizar (minimizar) as requisições da biblioteca **web3.py** a nós da rede.

Em redes blockchain, todo histórico de transações efetuadas é replicado em todos os nós. Para executar ou ler uma transação o ponto de entrada deve ser também um nó da rede. Por esse motivo torna-se necessário possuir acesso um nó na rede. Para realizar essa tarefa duas possibilidades se apresentam:

1. **Deploy de um nó próprio**: Isso pode ser feito em um servidor próprio ou em um provedor de cloud. A vantagem dessa abordagem é que o acesso a rede é direto e não há limitações de requisições. Porém, a desvantagem é que o custo de hardware para deploy de um nó é alto e a manutenção do mesmo também é custosa.

2. **Provedor de [Blockchain NaaS](https://cryptoapis.io/products/node-as-a-service)**: São serviços que oferecem acesso a nós de redes blockchain por meio de uma API. A vantagem dessa abordagem é que o custo é baixo e a manutenção é feita pelo provedor. A desvantagem é que há limitações de requisições.


Devido aos requisitos de hardware para deploy de um nó serem custosos, tanto financeiramente quanto tecnicamente, a primeira abordagem escolhida foi utilizar um provedor de Blockchain NaaS. Os principais provedores nesse ramo são [Infura](https://www.infura.io/) e [Alchemy](https://www.alchemy.com/), e por esse motivo ambos foram utilizados nessa aplicação.

### 2.4 - Breve overview sobre funcionamento de uma rede Blockchain

#### 2.4.1 - Estrutura de um bloco e transações

Resumidamente, em redes de blockchain [blocos](https://ethereum.org/pt-br/developers/docs/blocks/) são minerados periodicamente na cadeia de blocos. Cada bloco contém transações efetuadas por usuários do protocolo e validadas pelo nó que minerou aquele bloco. A cadeia de blocos é imutável e transparente a todos os nós da rede. Qualquer nó pode verificar as transações contidas em blocos minerados.

<img src="./img/chain-txs.png" alt="Figura 1: Blocos e transações" width="500"/>

**Conclusão**: Pelas razões acima, é possível obter dados brutos de transações efetuadas em redes blockchain por meio de requisições a nós pertencentes rede.

#### 2.4.2 - Contratos inteligentes e transações

Em redes blockchain EVM, transações podem ser de diferentes tipos. Uma transação pode ser:

- Transferência de token nativo da rede entre endereços;
- Interação com um contrato inteligente da rede;
- Deploy de um novo contrato inteligente.

Essas transações contém dados brutos com diferentes atributos que são bem úteis para inumeras aplicabilidades. Seguem alguns exemplos:

- Monitoramento de todas as transferências de um token nativo de uma rede blockchain, como por exemplo o ETH na rede Ethereum.
- Monitoramento de interações com todos os contratos inteligentes ja deployados na rede.
- Análise de contratos inteligentes recém deployados nas redes de teste e mainnets, podendo obter informações sobre aplicações desenvolvidas no protocolo.
- Análise das taxas de queima (burn) do token nativo, por meio de consumo de gas, e criação (mint), por meio de novos blocos minerados tirando insights sobre [tokenomics](https://academy.binance.com/en/articles/what-is-tokenomics-and-why-does-it-matter).
- Análise taxas cobradas para efetuar transações. 

<hr>

## 3 - Arquitetura de Solução

A seguir é apresentada uma breve descrição sobre as tecnologias utilizadas para construção desse sistema.

### 3.1 - Docker

O pacote **Onchain-stream-txs** é composto de 7 scripts python que executam de forma concorrente e colaborativa. Eles comportam-se como JOBS e lẽem dados da blockchain usando a biblioteca `web3.py`. Comunicam entre si por meio de tópicos do Apache Kafka e cada Job é executado dentro de um container, instanciado a partir de uma [imagem offchain-watcher](https://hub.docker.com/repository/docker/marcoaureliomenezes/offchain-watcher/general). Isso traz benefícios para o sistema como isolamento, escalabilidade e alta disponibilidade e portabilidade.

### 3.2 - Biblioteca Web3.py e a interação com provedores NaaS

Conforme descrito acima, usando a biblioteca web3.py é possivel se conectar a blockchains usando uma url e o protocolo HTTP. Essa requisição é feita por meio um provedor NaaS. Contudo, é necessário criar uma conta e gerar uma chave (API KEY) para acesso ao serviço. 

Segue abaixo um exemplo de como instanciar um objeto web3 com acesso a um nó da rede principal **ethereum** utilizando do provedor infura.

```python
web3 = Web3(Web3.HTTPProvider(https://mainnet.infura.io/v3/{api_key}))
```

**OBS:** Cada API Key, em seu plano gratuito, oferece:

- Número de requisições totais diárias limitadas;
- Taxa de requisições por segundo limitadas.

Essa informação é um fator limitante para o objetivo dessa aplicação. Portanto torna-se necessária a implementação de uma arquitetura de solução que:

1. Minimize o número de requisições para obter os dados de transações efetuadas em tempo real.
2. Balanceie o número de requisições entre diferentes API Keys.
3. Reduza a latência de ingestão dos dados.
4. Seja escalável e resiliente a falhas.

### 3.3 - Apache Kafka


<img src="./img/arquitetura.png" alt="Figura 2: Ilustração da arquitetura de solução" width="800"/>

A ilustração acima pode ser entendida como um sistema com diferentes partes que trabalhando junto e se comunicando via Kafka enviam os dados obtidos via web3.py api para o kafka.

### 3.4 - Workflow do sistema
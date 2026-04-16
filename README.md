# 🎬 Series Brazil Bot

Bot do Telegram para buscar e acompanhar **series e filmes** usando um catalogo web como fonte.

Baseado na estrutura do AnimesBaltigo Bot, adaptado para séries e filmes.

---

## 🚀 Instalação

### 1. Clone e entre no diretório
```bash
cd series-brazil-bot
```

### 2. Instale as dependências
```bash
pip install -r requirements.txt
```

### 3. Configure o `.env`
```bash
cp .env.example .env
# Edite o .env com seus dados
```

### 4. Variáveis obrigatórias no `.env`

| Variável | Descrição |
|---|---|
| `BOT_TOKEN` | Token do bot (obtido no @BotFather) |
| `ADMIN_IDS` | ID(s) do Telegram dos admins, separados por vírgula |
| `REQUIRED_CHANNEL` | Canal obrigatório p/ usar o bot (ex: `@MeuCanal`) |
| `REQUIRED_CHANNEL_URL` | URL do canal (ex: `t.me/MeuCanal`) |
| `BOT_USERNAME` | Username do bot sem `@` |
| `CANAL_POSTAGEM` | Canal onde conteúdo será postado (ex: `@MeuCanal`) |
| `BOT_BRAND` | Nome do bot para exibição |

### 5. Variáveis opcionais

| Variável | Descrição |
|---|---|
| `GROQ_API_KEY` | Chave da API Groq para IA nos grupos |
| `GEMINI_API_KEY` | Chave Gemini (alternativa à Groq) |

### 6. Rode o bot
```bash
python bot.py
```

---

## 📋 Comandos

### Usuários
| Comando | Descrição |
|---|---|
| `/start` | Iniciar o bot |
| `/buscar <nome>` | Buscar series e filmes no catalogo |
| `/ajuda` | Lista de comandos |
| `/indicacoes` | Seu link de indicação |
| `/pedido` | Central de pedidos |
| `/bingo` | Participar do bingo |

### Admins
| Comando | Descrição |
|---|---|
| `/postserie <nome>` | Posta uma série no canal |
| `/postfilme <nome>` | Posta um filme no canal |
| `/postnovoseps` | Verifica e posta conteúdo novo |
| `/broadcast` | Enviar mensagem para todos usuários |
| `/metricas` | Estatísticas do bot |
| `/refstats` | Estatísticas de indicações |
| `/startbingo` | Inicia o bingo |
| `/sortear` | Sorteia número do bingo |

---

## ⚙️ Como funciona

1. **Busca**: O bot faz scraping do catalogo via `services/catalog_client.py`
2. **Detalhes**: Ao clicar num resultado, carrega a pagina com sinopse, elenco e generos
3. **Episodios**: Para series, lista os episodios com links diretos para assistir
4. **Postagem automática**: O job `auto_post_new_eps_job` verifica novos conteúdos a cada 10 min e posta no canal
5. **IA nos grupos**: Quando alguém menciona "akira" no grupo, o bot responde usando Groq/LLaMA

---

## 📁 Estrutura

```
series-brazil-bot/
├── bot.py                    # Ponto de entrada
├── config.py                 # Configurações
├── requirements.txt
├── .env.example
├── handlers/
│   ├── search.py             # /buscar
│   ├── callbacks.py          # Botões inline (detalhes, episódios, temporadas)
│   ├── start.py              # /start
│   ├── postanime.py          # /postserie (admin)
│   ├── postfilmes.py         # /postfilme (admin)
│   ├── novoseps.py           # /postnovoseps + job automático
│   ├── broadcast.py          # /broadcast
│   ├── referral.py           # /indicacoes
│   ├── referral_admin.py     # /refstats
│   ├── bingo.py              # /bingo
│   ├── bingo_admin.py        # /startbingo /sortear
│   ├── metricas.py           # /metricas
│   ├── group_ai.py           # IA "Akira" nos grupos
│   ├── inline.py             # Busca inline
│   └── ...
├── services/
│   ├── catalog_client.py     # 🔑 Cliente principal do catalogo
│   ├── metrics.py            # Banco de métricas (SQLite)
│   ├── referral_db.py        # Sistema de indicações (SQLite)
│   ├── bingo_system.py       # Lógica do bingo
│   ├── gemini_ai.py          # Integração Groq/Gemini
│   ├── memory.py             # Memória de conversas (IA)
│   └── user_registry.py      # Registro de usuários
└── utils/
    └── gatekeeper.py         # Verificação de canal
```

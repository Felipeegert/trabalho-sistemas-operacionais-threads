#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

/* ------------------------------
   CONFIGURAÇÕES (ligar/desligar)
   ------------------------------ */
#define ENABLE_DETECTION    true   // parte 2: detectar deadlocks a cada 5s
#define ENABLE_RESOLUTION   true   // parte 2: ao detectar, resolver (marca vítima)
#define ENABLE_PREVENTION   true   // parte 3: prevenção para threads das empresas

/* ------------------------------
   Constantes e nomes
   ------------------------------ */
const char *logs[] = { "Interface", "Input", "Operacao", "Localizacao", "Propaganda", "Calculo" };

const char *nomeArquivos[] = {
    "buffer.log",       // 0
    "Interface.log",    // 1
    "Input.log",        // 2
    "Operacao.log",     // 3
    "Localizacao.log",  // 4
    "Propaganda.log",   // 5
    "Calculo.log",      // 6
    "Omega.log",        // 7  (empresa: receberá dados)
    "KleubsMax.log",    // 8
    "ChirpTome.log"     // 9
};

#define nTipoLogs 6
#define nArquivos 10
#define nThreads 9

/* ------------------------------
   Estruturas para arquivos e threads
   ------------------------------ */
typedef struct {
    int id;
    pthread_mutex_t mutex;
} ArquivoData;

typedef struct {
    pthread_t thread;            // id do pthread
    bool stop;                   // flag para parar thread
    bool holding[nArquivos];     // quais recursos está segurando no momento
    int waiting_for;             // índice do recurso que está esperando (-1 = nenhum)
    bool aborted;                // usado para resolver deadlock: thread limpa e reinicia
    int tid;                     // index lógico da thread (0..nThreads-1) para logs
} ThreadData;

/* ------------------------------
   Variáveis globais
   ------------------------------ */
ArquivoData arquivos[nArquivos];
ThreadData threads[nThreads]; // 9 threads: buffer, interface, operacao, localizacao, propaganda, calculo, omega, kleubsmax, chirptome

enum {
    T_BUFFER,
    T_INTERFACE,
    T_OPERACAO,
    T_LOCALIZACAO,
    T_PROPAGANDA,
    T_CALCULO,
    T_OMEGA,
    T_KLEUBSMAX,
    T_CHIRPTOME
};

/* ------------------------------
   Protótipos
   ------------------------------ */
void geraLogs(const char *name);
void moveLog_trylock_style(int srcIdx, int dstIdx, const char *palavra, ThreadData *t);
void LogThread(ThreadData *t);
void mark_hold(ThreadData *t, int idx, bool value);
bool try_acquire(pthread_mutex_t *m);
bool try_acquire_file(int idx, ThreadData *t);
void release_all_holdings(ThreadData *t);
void *ThreadBuffer(void *arg);
void *ThreadInterface(void *arg);
void *ThreadOperacao(void *arg);
void *ThreadLocalizacao(void *arg);
void *ThreadPropaganda(void *arg);
void *ThreadCalculo(void *arg);
void *ThreadOmega(void *arg);
void *ThreadKleubsMax(void *arg);
void *ThreadChirpTome(void *arg);
void *DetectorDeadlockThread(void *arg);
bool detect_deadlock(int involvedThreads[], int *nInvolved);

/* ------------------------------
   Função principal
   ------------------------------ */
int main() {
    srand(time(NULL));
    printf("Iniciando sistema de logs (detecção=%d, resolução=%d, prevenção=%d)\n",
           ENABLE_DETECTION, ENABLE_RESOLUTION, ENABLE_PREVENTION);

    // inicializa arquivos e mutexes
    for (int i = 0; i < nArquivos; i++) {
        arquivos[i].id = i;
        pthread_mutex_init(&arquivos[i].mutex, NULL);
    }

    // inicializa estado das threads
    for (int i = 0; i < nThreads; i++) {
        threads[i].stop = false;
        threads[i].waiting_for = -1;
        threads[i].aborted = false;
        threads[i].tid = i;
        for (int j = 0; j < nArquivos; j++) threads[i].holding[j] = false;
    }

    // cria as threads de trabalho
    pthread_create(&threads[T_BUFFER].thread, NULL, ThreadBuffer, &threads[T_BUFFER]);
    pthread_create(&threads[T_INTERFACE].thread, NULL, ThreadInterface, &threads[T_INTERFACE]);
    pthread_create(&threads[T_OPERACAO].thread, NULL, ThreadOperacao, &threads[T_OPERACAO]);
    pthread_create(&threads[T_LOCALIZACAO].thread, NULL, ThreadLocalizacao, &threads[T_LOCALIZACAO]);
    pthread_create(&threads[T_PROPAGANDA].thread, NULL, ThreadPropaganda, &threads[T_PROPAGANDA]);
    pthread_create(&threads[T_CALCULO].thread, NULL, ThreadCalculo, &threads[T_CALCULO]);
    pthread_create(&threads[T_OMEGA].thread, NULL, ThreadOmega, &threads[T_OMEGA]);
    pthread_create(&threads[T_KLEUBSMAX].thread, NULL, ThreadKleubsMax, &threads[T_KLEUBSMAX]);
    pthread_create(&threads[T_CHIRPTOME].thread, NULL, ThreadChirpTome, &threads[T_CHIRPTOME]);

    // cria detector de deadlock (opcional)
    pthread_t detector;
    if (ENABLE_DETECTION) {
        pthread_create(&detector, NULL, DetectorDeadlockThread, NULL);
    }

    // deixa rodar um tempo (para demo). Em apresentação, adaptar para parar conforme necessário
    sleep(30); // tempo maior para observar detecção e prevenção

    // sinaliza parada
    for (int i = 0; i < nThreads; i++) threads[i].stop = true;

    // aguarda término
    for (int i = 0; i < nThreads; i++) pthread_join(threads[i].thread, NULL);

    if (ENABLE_DETECTION) pthread_cancel(detector); // detector pode estar em sleep
    if (ENABLE_DETECTION) pthread_join(detector, NULL);

    printf("Execução finalizada.\n");
    return 0;
}

/* ------------------------------
   FUNÇÕES AUXILIARES: marcações e try-acquire
   - Mantém arrays de 'holding' e 'waiting_for' por thread
   - Usa pthread_mutex_trylock para não bloquear indefinidamente
   ------------------------------ */

void mark_hold(ThreadData *t, int idx, bool value) {
    if (idx >= 0 && idx < nArquivos) t->holding[idx] = value;
}

bool try_acquire(pthread_mutex_t *m) {
    return (pthread_mutex_trylock(m) == 0);
}

bool try_acquire_file(int idx, ThreadData *t) {
    // tenta adquirir o mutex do arquivo 'idx'
    if (try_acquire(&arquivos[idx].mutex)) {
        mark_hold(t, idx, true);
        return true;
    } else {
        return false;
    }
}

void release_all_holdings(ThreadData *t) {
    // libera todos os mutexes que esta thread diz segurar
    for (int i = 0; i < nArquivos; i++) {
        if (t->holding[i]) {
            // libere e marque como não segurando
            pthread_mutex_unlock(&arquivos[i].mutex);
            t->holding[i] = false;
            printf("[Thread %d] liberou arquivo %s (release_all_holdings)\n", t->tid, nomeArquivos[i]);
        }
    }
}

/* ------------------------------
   GERA LOGS: adiciona 5 linhas aleatórias ao buffer
   ------------------------------ */
void geraLogs(const char *name) {
    FILE *f = fopen(name, "a");
    if (!f) {
        // cria arquivo se não existir
        f = fopen(name, "w");
        if (!f) return;
    }
    for (int i = 0; i < 5; i++) {
        fprintf(f, "%s\n", logs[rand() % nTipoLogs]);
    }
    fclose(f);
}

/* ------------------------------
   moveLog_trylock_style:
   - lê 'origem' (srcIdx) e copia linhas que contenham 'palavra' para destino (dstIdx)
   - função espera ter exclusividade nos arquivos apropriados
   - Implementada para ser chamada quando os locks já foram obtidos
   ------------------------------ */
void moveLog_trylock_style(int srcIdx, int dstIdx, const char *palavra, ThreadData *t) {
    // pressupõe que mutexes de srcIdx e dstIdx já estejam segurados por esta thread
    char tmpName[128];
    snprintf(tmpName, sizeof(tmpName), "%s_temp", nomeArquivos[srcIdx]);

    FILE *fin = fopen(nomeArquivos[srcIdx], "r");
    FILE *fout = fopen(nomeArquivos[dstIdx], "a");
    FILE *ftmp = fopen(tmpName, "w");
    if (!fin) {
        // nada a fazer se não existe arquivo de origem
        if (fout) fclose(fout);
        if (ftmp) fclose(ftmp);
        return;
    }
    if (!fout) fout = fopen(nomeArquivos[dstIdx], "a"); // tentativa extra
    if (!ftmp) ftmp = fopen(tmpName, "w"); // tentativa extra
    if (!fin || !fout || !ftmp) {
        if (fin) fclose(fin);
        if (fout) fclose(fout);
        if (ftmp) fclose(ftmp);
        return;
    }

    char linha[256];
    while (fgets(linha, sizeof(linha), fin)) {
        if (strstr(linha, palavra) != NULL) {
            fputs(linha, fout);
        } else {
            fputs(linha, ftmp);
        }
    }
    fclose(fin);
    fclose(fout);
    fclose(ftmp);

    // substitui arquivo de origem
    remove(nomeArquivos[srcIdx]);
    rename(tmpName, nomeArquivos[srcIdx]);

    printf("[Thread %d] moveu linhas '%s' de %s para %s\n", t->tid, palavra, nomeArquivos[srcIdx], nomeArquivos[dstIdx]);
}

/* ------------------------------
   LogThread: imprime estado (quem segura o que e esperando por qual recurso)
   ------------------------------ */
void LogThread(ThreadData *t) {
    printf("[Thread %d] holding:", t->tid);
    for (int i = 0; i < nArquivos; i++) if (t->holding[i]) printf(" %s", nomeArquivos[i]);
    printf(" | waiting_for: %s\n", (t->waiting_for == -1) ? "none" : nomeArquivos[t->waiting_for]);
}

/* ------------------------------
   THREAD: Buffer - gera logs no arquivo buffer.log
   - Usa lock no arquivo buffer somente (trylock loop)
   ------------------------------ */
void *ThreadBuffer(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    t->tid = T_BUFFER;

    while (!t->stop) {
        // tenta adquirir o buffer
        t->waiting_for = 0; // buffer index
        while (!t->stop && !try_acquire_file(0, t)) {
            // espera breve e repete
            usleep(100 * 1000);
            if (t->aborted) { // caso resolução peça abortar
                release_all_holdings(t);
                t->aborted = false;
            }
        }
        t->waiting_for = -1;

        // temos o buffer
        LogThread(t);
        geraLogs(nomeArquivos[0]);

        // libera buffer
        if (t->holding[0]) {
            pthread_mutex_unlock(&arquivos[0].mutex);
            t->holding[0] = false;
        }

        sleep(1); // espera 1s conforme enunciado
    }
    // limpeza final
    release_all_holdings(t);
    return NULL;
}

/* ------------------------------
   THREADS que movem logs do buffer para arquivos específicos
   - Operam sempre tentando obter buffer(0) e depois o destino.
   - Para evitar starvation e facilitar detecção, usamos trylock loop.
   ------------------------------ */

void *ThreadInterface(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    t->tid = T_INTERFACE;
    int src = 0, dst = 1;

    while (!t->stop) {
        // Se prevenção ativada, precisa de todos os locks simultâneos (acquire-all-or-none)
        if (ENABLE_PREVENTION) {
            // ordem crescente por índice para evitar inversões (lock-ordering)
            int ids[2] = { src, dst };
            // tentar adquirir todos
            bool all_acquired = false;
            while (!t->stop && !all_acquired) {
                all_acquired = true;
                // tenta pegar cada mutex
                for (int i = 0; i < 2; i++) {
                    int idx = ids[i];
                    t->waiting_for = idx;
                    if (!try_acquire_file(idx, t)) {
                        all_acquired = false;
                        // libera qualquer que já tiver obtido nessa tentativa
                        for (int j = 0; j < nArquivos; j++) {
                            if (t->holding[j]) {
                                pthread_mutex_unlock(&arquivos[j].mutex);
                                t->holding[j] = false;
                            }
                        }
                        t->waiting_for = -1;
                        usleep(100 * 1000);
                        break;
                    } else {
                        t->waiting_for = -1;
                    }
                }
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
            // se cancelado, continue
            if (t->stop) break;
        } else {
            // modo padrão (trylock sequencial: buffer -> dest)
            // 1) pegar buffer
            t->waiting_for = src;
            while (!t->stop && !try_acquire_file(src, t)) {
                usleep(100 * 1000);
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
            t->waiting_for = -1;
            // 2) pegar destino
            t->waiting_for = dst;
            while (!t->stop && !try_acquire_file(dst, t)) {
                usleep(100 * 1000);
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
            t->waiting_for = -1;
        }

        if (t->stop) break;

        // agora tenho ambos
        LogThread(t);
        moveLog_trylock_style(src, dst, "Interface", t);

        // libera ambos
        release_all_holdings(t);

        sleep(1);
    }

    release_all_holdings(t);
    return NULL;
}

void *ThreadOperacao(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    t->tid = T_OPERACAO;
    int src = 0, dst = 3;

    while (!t->stop) {
        if (ENABLE_PREVENTION) {
            int ids[2] = { src, dst };
            bool all_acquired = false;
            while (!t->stop && !all_acquired) {
                all_acquired = true;
                for (int i = 0; i < 2; i++) {
                    int idx = ids[i];
                    t->waiting_for = idx;
                    if (!try_acquire_file(idx, t)) {
                        all_acquired = false;
                        for (int j = 0; j < nArquivos; j++) {
                            if (t->holding[j]) { pthread_mutex_unlock(&arquivos[j].mutex); t->holding[j] = false; }
                        }
                        t->waiting_for = -1;
                        usleep(100 * 1000);
                        break;
                    } else {
                        t->waiting_for = -1;
                    }
                }
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
            if (t->stop) break;
        } else {
            t->waiting_for = src;
            while (!t->stop && !try_acquire_file(src, t)) { usleep(100 * 1000); if (t->aborted) { release_all_holdings(t); t->aborted = false; } }
            t->waiting_for = -1;
            t->waiting_for = dst;
            while (!t->stop && !try_acquire_file(dst, t)) { usleep(100 * 1000); if (t->aborted) { release_all_holdings(t); t->aborted = false; } }
            t->waiting_for = -1;
        }

        if (t->stop) break;

        LogThread(t);
        moveLog_trylock_style(src, dst, "Operacao", t);

        release_all_holdings(t);
        sleep(1);
    }

    release_all_holdings(t);
    return NULL;
}

void *ThreadLocalizacao(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    t->tid = T_LOCALIZACAO;
    int src = 0, dst = 4;

    while (!t->stop) {
        if (ENABLE_PREVENTION) {
            int ids[2] = { src, dst };
            bool all_acquired = false;
            while (!t->stop && !all_acquired) {
                all_acquired = true;
                for (int i = 0; i < 2; i++) {
                    int idx = ids[i];
                    t->waiting_for = idx;
                    if (!try_acquire_file(idx, t)) {
                        all_acquired = false;
                        for (int j = 0; j < nArquivos; j++) {
                            if (t->holding[j]) { pthread_mutex_unlock(&arquivos[j].mutex); t->holding[j] = false; }
                        }
                        t->waiting_for = -1;
                        usleep(100 * 1000);
                        break;
                    } else {
                        t->waiting_for = -1;
                    }
                }
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
            if (t->stop) break;
        } else {
            t->waiting_for = src;
            while (!t->stop && !try_acquire_file(src, t)) { usleep(100 * 1000); if (t->aborted) { release_all_holdings(t); t->aborted = false; } }
            t->waiting_for = -1;
            t->waiting_for = dst;
            while (!t->stop && !try_acquire_file(dst, t)) { usleep(100 * 1000); if (t->aborted) { release_all_holdings(t); t->aborted = false; } }
            t->waiting_for = -1;
        }

        if (t->stop) break;

        LogThread(t);
        moveLog_trylock_style(src, dst, "Localizacao", t);

        release_all_holdings(t);
        sleep(1);
    }

    release_all_holdings(t);
    return NULL;
}

void *ThreadPropaganda(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    t->tid = T_PROPAGANDA;
    int src = 0, dst = 5;

    while (!t->stop) {
        if (ENABLE_PREVENTION) {
            int ids[2] = { src, dst };
            bool all_acquired = false;
            while (!t->stop && !all_acquired) {
                all_acquired = true;
                for (int i = 0; i < 2; i++) {
                    int idx = ids[i];
                    t->waiting_for = idx;
                    if (!try_acquire_file(idx, t)) {
                        all_acquired = false;
                        for (int j = 0; j < nArquivos; j++) {
                            if (t->holding[j]) { pthread_mutex_unlock(&arquivos[j].mutex); t->holding[j] = false; }
                        }
                        t->waiting_for = -1;
                        usleep(100 * 1000);
                        break;
                    } else {
                        t->waiting_for = -1;
                    }
                }
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
            if (t->stop) break;
        } else {
            t->waiting_for = src;
            while (!t->stop && !try_acquire_file(src, t)) { usleep(100 * 1000); if (t->aborted) { release_all_holdings(t); t->aborted = false; } }
            t->waiting_for = -1;
            t->waiting_for = dst;
            while (!t->stop && !try_acquire_file(dst, t)) { usleep(100 * 1000); if (t->aborted) { release_all_holdings(t); t->aborted = false; } }
            t->waiting_for = -1;
        }

        if (t->stop) break;

        LogThread(t);
        moveLog_trylock_style(src, dst, "Propaganda", t);

        release_all_holdings(t);
        sleep(1);
    }

    release_all_holdings(t);
    return NULL;
}

void *ThreadCalculo(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    t->tid = T_CALCULO;
    int src = 0, dst = 6;

    while (!t->stop) {
        if (ENABLE_PREVENTION) {
            int ids[2] = { src, dst };
            bool all_acquired = false;
            while (!t->stop && !all_acquired) {
                all_acquired = true;
                for (int i = 0; i < 2; i++) {
                    int idx = ids[i];
                    t->waiting_for = idx;
                    if (!try_acquire_file(idx, t)) {
                        all_acquired = false;
                        for (int j = 0; j < nArquivos; j++) {
                            if (t->holding[j]) { pthread_mutex_unlock(&arquivos[j].mutex); t->holding[j] = false; }
                        }
                        t->waiting_for = -1;
                        usleep(100 * 1000);
                        break;
                    } else {
                        t->waiting_for = -1;
                    }
                }
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
            if (t->stop) break;
        } else {
            t->waiting_for = src;
            while (!t->stop && !try_acquire_file(src, t)) { usleep(100 * 1000); if (t->aborted) { release_all_holdings(t); t->aborted = false; } }
            t->waiting_for = -1;
            t->waiting_for = dst;
            while (!t->stop && !try_acquire_file(dst, t)) { usleep(100 * 1000); if (t->aborted) { release_all_holdings(t); t->aborted = false; } }
            t->waiting_for = -1;
        }

        if (t->stop) break;

        LogThread(t);
        moveLog_trylock_style(src, dst, "Calculo", t);

        release_all_holdings(t);
        sleep(1);
    }

    release_all_holdings(t);
    return NULL;
}

/* ------------------------------
   THREADS DAS EMPRESAS
   - Omega: Operacao (3), Propaganda (5), Calculo (6)
   - KleubsMax: Propaganda (5), Interface (1), Localizacao (4)
   - ChirpTome: Calculo (6), Localizacao (4), Input (2)  (input permanece no buffer)
   Implementadas para demonstrar prevenção (acquire-all-or-none) e para serem
   parte da detecção (esperando recursos).
   ------------------------------ */

void *ThreadOmega(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    t->tid = T_OMEGA;
    int needed[] = { 3, 5, 6 }; // Operacao, Propaganda, Calculo
    int nneeded = 3;

    while (!t->stop) {
        // Se prevenção ativada: acquire-all-or-none, ordenando índices
        bool acquired = false;
        if (ENABLE_PREVENTION) {
            // ordena os ids para lock-ordering (simples bubble - tamanho pequeno)
            int ids[3]; memcpy(ids, needed, sizeof(ids));
            for (int i = 0; i < nneeded-1; i++)
                for (int j = i+1; j < nneeded; j++)
                    if (ids[i] > ids[j]) { int tmp = ids[i]; ids[i] = ids[j]; ids[j] = tmp; }

            while (!t->stop && !acquired) {
                acquired = true;
                for (int i = 0; i < nneeded; i++) {
                    t->waiting_for = ids[i];
                    if (!try_acquire_file(ids[i], t)) {
                        // falhou: libera o que pegou nesta tentativa
                        acquired = false;
                        for (int j = 0; j < nArquivos; j++) { if (t->holding[j]) { pthread_mutex_unlock(&arquivos[j].mutex); t->holding[j] = false; } }
                        t->waiting_for = -1;
                        usleep(150 * 1000);
                        break;
                    } else {
                        t->waiting_for = -1;
                    }
                }
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
        } else {
            // modo sem prevenção: tenta pegar recursos sequencialmente (pode gerar ciclos)
            for (int i = 0; i < nneeded && !t->stop; i++) {
                t->waiting_for = needed[i];
                while (!t->stop && !try_acquire_file(needed[i], t)) {
                    usleep(120 * 1000);
                    if (t->aborted) { release_all_holdings(t); t->aborted = false; }
                }
                t->waiting_for = -1;
            }
        }

        if (t->stop) break;

        // simula leitura/agregação e escrita para o arquivo da empresa
        LogThread(t);
        // por demonstração: copia conteúdo dos arquivos de interesse para Omega.log
        FILE *out = fopen(nomeArquivos[7], "a");
        if (out) {
            fprintf(out, "[Omega] coletou dados às %ld\n", time(NULL));
            fclose(out);
        }
        printf("[Omega] coletou dados.\n");

        release_all_holdings(t);
        sleep(2);
    }

    release_all_holdings(t);
    return NULL;
}

void *ThreadKleubsMax(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    t->tid = T_KLEUBSMAX;
    int needed[] = { 5, 1, 4 }; // Propaganda, Interface, Localizacao
    int nneeded = 3;

    while (!t->stop) {
        bool acquired = false;
        if (ENABLE_PREVENTION) {
            int ids[3]; memcpy(ids, needed, sizeof(ids));
            for (int i = 0; i < nneeded-1; i++)
                for (int j = i+1; j < nneeded; j++)
                    if (ids[i] > ids[j]) { int tmp = ids[i]; ids[i] = ids[j]; ids[j] = tmp; }

            while (!t->stop && !acquired) {
                acquired = true;
                for (int i = 0; i < nneeded; i++) {
                    t->waiting_for = ids[i];
                    if (!try_acquire_file(ids[i], t)) {
                        acquired = false;
                        for (int j = 0; j < nArquivos; j++) { if (t->holding[j]) { pthread_mutex_unlock(&arquivos[j].mutex); t->holding[j] = false; } }
                        t->waiting_for = -1;
                        usleep(150 * 1000);
                        break;
                    } else {
                        t->waiting_for = -1;
                    }
                }
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
        } else {
            for (int i = 0; i < nneeded && !t->stop; i++) {
                t->waiting_for = needed[i];
                while (!t->stop && !try_acquire_file(needed[i], t)) {
                    usleep(120 * 1000);
                    if (t->aborted) { release_all_holdings(t); t->aborted = false; }
                }
                t->waiting_for = -1;
            }
        }

        if (t->stop) break;

        LogThread(t);
        FILE *out = fopen(nomeArquivos[8], "a");
        if (out) {
            fprintf(out, "[KleubsMax] coletou dados às %ld\n", time(NULL));
            fclose(out);
        }
        printf("[KleubsMax] coletou dados.\n");

        release_all_holdings(t);
        sleep(2);
    }

    release_all_holdings(t);
    return NULL;
}

void *ThreadChirpTome(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    t->tid = T_CHIRPTOME;
    int needed[] = { 6, 4, 2 }; // Calculo, Localizacao, Input(buffer)
    int nneeded = 3;

    while (!t->stop) {
        bool acquired = false;
        if (ENABLE_PREVENTION) {
            int ids[3]; memcpy(ids, needed, sizeof(ids));
            for (int i = 0; i < nneeded-1; i++)
                for (int j = i+1; j < nneeded; j++)
                    if (ids[i] > ids[j]) { int tmp = ids[i]; ids[i] = ids[j]; ids[j] = tmp; }

            while (!t->stop && !acquired) {
                acquired = true;
                for (int i = 0; i < nneeded; i++) {
                    t->waiting_for = ids[i];
                    if (!try_acquire_file(ids[i], t)) {
                        acquired = false;
                        for (int j = 0; j < nArquivos; j++) { if (t->holding[j]) { pthread_mutex_unlock(&arquivos[j].mutex); t->holding[j] = false; } }
                        t->waiting_for = -1;
                        usleep(150 * 1000);
                        break;
                    } else {
                        t->waiting_for = -1;
                    }
                }
                if (t->aborted) { release_all_holdings(t); t->aborted = false; }
            }
        } else {
            for (int i = 0; i < nneeded && !t->stop; i++) {
                t->waiting_for = needed[i];
                while (!t->stop && !try_acquire_file(needed[i], t)) {
                    usleep(120 * 1000);
                    if (t->aborted) { release_all_holdings(t); t->aborted = false; }
                }
                t->waiting_for = -1;
            }
        }

        if (t->stop) break;

        LogThread(t);
        FILE *out = fopen(nomeArquivos[9], "a");
        if (out) {
            fprintf(out, "[ChirpTome] coletou dados às %ld\n", time(NULL));
            fclose(out);
        }
        printf("[ChirpTome] coletou dados.\n");

        release_all_holdings(t);
        sleep(2);
    }

    release_all_holdings(t);
    return NULL;
}

/* ------------------------------
   DETECTOR DE DEADLOCK (rodando em thread separada)
   - Constrói grafo: thread -> resource (waiting_for) e resource -> thread (holding)
   - Procura ciclos: thread -> resource -> thread -> resource ...
   - Se detecta ciclo, imprime envolvidos e resolve (marcando victim.aborted = true)
   - Retorna lista de threads envolvidos via involvedThreads
   ------------------------------ */
bool detect_deadlock(int involvedThreads[], int *nInvolved) {
    // Representação simplificada:
    // - threads[i].waiting_for = r  => edge T_i -> R_r
    // - some thread j holds resource r => edge R_r -> T_j
    // Procuramos ciclo que contenha pelo menos um thread.

    *nInvolved = 0;
    // construímos arrays locais de posse rápidos:
    int owner_of_resource[nArquivos];
    for (int r = 0; r < nArquivos; r++) owner_of_resource[r] = -1;
    for (int t = 0; t < nThreads; t++) {
        for (int r = 0; r < nArquivos; r++) {
            if (threads[t].holding[r]) {
                owner_of_resource[r] = t;
            }
        }
    }

    // Para cada thread, percorremos caminho thread->resource->thread->...
    // Se chegarmos novamente ao mesmo thread, ciclo detectado.
    bool visitedT[nThreads];
    for (int i = 0; i < nThreads; i++) visitedT[i] = false;

    for (int start = 0; start < nThreads; start++) {
        if (threads[start].waiting_for == -1) continue; // não esperando
        int curThread = start;
        bool seenThread[nThreads]; for (int k=0;k<nThreads;k++) seenThread[k]=false;
        while (true) {
            if (seenThread[curThread]) {
                // ciclo envolvendo curThread encontrado; colect all threads in cycle by scanning again
                // Para simplicidade, recolhemos threads que estão no caminho (seenThread==true)
                int count = 0;
                for (int t = 0; t < nThreads; t++) {
                    if (seenThread[t]) {
                        involvedThreads[count++] = t;
                    }
                }
                *nInvolved = count;
                return true;
            }
            seenThread[curThread] = true;
            int res = threads[curThread].waiting_for;
            if (res == -1) break; // este thread não espera por recurso — caminho termina

            int owner = owner_of_resource[res];
            if (owner == -1) break; // recurso não é segurado por ninguém -> caminho termina

            // advance: owner is next thread
            curThread = owner;
        }
    }
    return false;
}

void *DetectorDeadlockThread(void *arg) {
    (void)arg;
    while (1) {
        sleep(5); // detectar a cada 5s conforme enunciado

        int involved[nThreads];
        int nInvolved = 0;
        bool dead = detect_deadlock(involved, &nInvolved);
        if (!dead) {
            printf("[DETECTOR] Sem deadlock no momento.\n");
            continue;
        }

        // imprime quais threads estão envolvidas
        printf("[DETECTOR] Deadlock detectado envolvendo %d threads:\n", nInvolved);
        for (int i = 0; i < nInvolved; i++) {
            int tid = involved[i];
            printf("   -> Thread %d (waiting_for=%s)\n", tid,
                   (threads[tid].waiting_for == -1) ? "none" : nomeArquivos[threads[tid].waiting_for]);
        }

        if (ENABLE_RESOLUTION) {
            // resolvemos escolhendo uma vítima simples: a thread com maior índice entre os envolvidos
            int victim = involved[0];
            for (int i = 1; i < nInvolved; i++) if (involved[i] > victim) victim = involved[i];

            printf("[DETECTOR] Resolvendo deadlock: escolhida thread %d como vítima. Indicando abort.\n", victim);
            // marca para que a thread faça liberação voluntária na próxima iteração
            threads[victim].aborted = true;

            // aguarda um curto período para que thread acabe de liberar
            sleep(1);

            // imprime estado pós-resolução
            printf("[DETECTOR] Pós-resolução: checando estado de threads:\n");
            for (int t = 0; t < nThreads; t++) {
                printf("   Thread %d: waiting_for=%d | holdings:", t, threads[t].waiting_for);
                for (int r=0;r<nArquivos;r++) if (threads[t].holding[r]) printf(" %s", nomeArquivos[r]);
                printf("\n");
            }
        } else {
            printf("[DETECTOR] Detecção ativa, resolução desativada (configuração).\n");
        }
    }
    return NULL;
}
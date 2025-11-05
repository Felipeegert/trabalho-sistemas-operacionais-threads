#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

// Variáveis de tipos e arquivos
const char *logs[]         = { "Interface", "Input", "Operacao", "Localizacao", "Propaganda", "Calculo" };
const char *nomeArquivos[] = { "buffer.log",
                               "Interface.log", "Input.log", "Operacao.log", "Localizacao.log", "Propaganda.log", "Calculo.log",
                               "Omega.log", "KleubsMax.log", "ChirpTome.log"};
#define nTipoLogs 6
#define nArquivos 10
#define nThreads 9 //////////////////////////////felipe

// Funções e structs auxiliares
void moveLog(const char *origem, const char *destino, const char *palavra);
void geraLogs(const char *name);

void *ThreadBuffer(void *arg);//////////////felipe
void *ThreadInterface(void *arg);/////////////felipe
void *ThreadOperacao(void *arg);/////////////felipe
void *ThreadLocalizacao(void *arg);/////////////felipe
void *ThreadPropaganda(void *arg);/////////////felipe
void *ThreadCalculo(void *arg);/////////////felipe
void *ThreadOmega(void *arg);/////////////felipe
void *ThreadKleubsMax(void *arg);/////////////felipe
void *ThreadChirpTome(void *arg);/////////////felipe


typedef struct {
    int id;
    pthread_mutex_t mutex;
} ArquivoData;

typedef struct {
    pthread_t thread;
    bool stop;
    bool acessando[nArquivos]; // true se acessando
} ThreadData;

void LogThread(ThreadData *t);

// Funções das threads
void *ThreadBuffer(void *arg);

// Dados globais
ArquivoData arquivos[nArquivos];
ThreadData threadBuffer;
ThreadData threads[nThreads]; ///////////////////////////////felipe

enum { //////////////////////////felipe
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

int main() {
    srand(time(NULL));

    printf("Iniciando...\n");

    // Inicializa os arquivos e mutexes
    for (int i = 0; i < nArquivos; i++) {
        arquivos[i].id = i;
    }
    for (int i = 0; i < nThreads; i++) { ////////////////////////////////////////////felipe
        threads[i].stop = false;
        for (int j = 0; j < nArquivos; j++)
            threads[i].acessando[j] = false;
    }
    for (int i = 0; i < nArquivos; i++){/////////////////////////////////felipe
        pthread_mutex_init(&arquivos[i].mutex, NULL);}

    // Inicializa thread de buffer
    for (int i = 0; i < nArquivos; i++)
        threadBuffer.acessando[i] = false;
    threadBuffer.stop = false;
    pthread_create(&threadBuffer.thread, NULL, ThreadBuffer, &threadBuffer); // arquivo 0 = buffer
    pthread_create(&threads[T_BUFFER].thread, NULL, ThreadBuffer, &threads[T_BUFFER]);//////////felipe
    pthread_create(&threads[T_INTERFACE].thread, NULL, ThreadInterface, &threads[T_INTERFACE]);//////////felipe
    pthread_create(&threads[T_OPERACAO].thread, NULL, ThreadOperacao, &threads[T_OPERACAO]);//////////felipe
    pthread_create(&threads[T_LOCALIZACAO].thread, NULL, ThreadLocalizacao, &threads[T_LOCALIZACAO]);//////////felipe
    pthread_create(&threads[T_PROPAGANDA].thread, NULL, ThreadPropaganda, &threads[T_PROPAGANDA]);//////////felipe
    pthread_create(&threads[T_CALCULO].thread, NULL, ThreadCalculo, &threads[T_CALCULO]);//////////felipe
    pthread_create(&threads[T_OMEGA].thread, NULL, ThreadOmega, &threads[T_OMEGA]);//////////felipe
    pthread_create(&threads[T_KLEUBSMAX].thread, NULL, ThreadKleubsMax, &threads[T_KLEUBSMAX]);//////////felipe
    pthread_create(&threads[T_CHIRPTOME].thread, NULL, ThreadChirpTome, &threads[T_CHIRPTOME]);//////////felipe



    // Deixa rodar por 10 segundos /////felipe
    sleep(10);

    for (int i = 0; i < nThreads; i++)
    threads[i].stop = true;
    
    for (int i = 0; i < nThreads; i++)
    pthread_join(threads[i].thread, NULL);
    
    
   

    printf("Finalizando.\n");

    return 0;
}

///////////////////////
// Funções das Threads
void *ThreadBuffer(void *arg) {

    ThreadData* myself = (ThreadData*)arg;
    ArquivoData *d = &arquivos[0];

    while(!myself->stop) {
        pthread_mutex_lock(&d->mutex);
        threadBuffer.acessando[0] = true;

        LogThread(myself);
        geraLogs(nomeArquivos[d->id]);

        threadBuffer.acessando[0] = false;
        pthread_mutex_unlock(&d->mutex);

        sleep(1);
    }
    return NULL;
}
/////////////////////// criador de arquivos felipe

void *ThreadInterface(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    ArquivoData *buffer = &arquivos[0];
    ArquivoData *dest = &arquivos[1]; // Interface.log

    while (!t->stop) {
        ///////trava os dois arquivos
        pthread_mutex_lock(&buffer->mutex);
        pthread_mutex_lock(&dest->mutex);

        t->acessando[0] = true; // buffer
        t->acessando[1] = true; // interface

        LogThread(t);
        moveLog(nomeArquivos[0], nomeArquivos[1], "Interface");

        t->acessando[0] = false;
        t->acessando[1] = false;

        //////// destrava os dois arquivos
        pthread_mutex_unlock(&dest->mutex);
        pthread_mutex_unlock(&buffer->mutex);

        sleep(1);
    }
    return NULL;
}

void *ThreadOperacao(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    ArquivoData *buffer = &arquivos[0];
    ArquivoData *dest = &arquivos[3]; // Operacao.log

    while (!t->stop) {
        pthread_mutex_lock(&buffer->mutex);
        pthread_mutex_lock(&dest->mutex);

        t->acessando[0] = true;
        t->acessando[3] = true;
        LogThread(t);
        moveLog(nomeArquivos[0], nomeArquivos[3], "Operacao");
        t->acessando[0] = false;
        t->acessando[3] = false;

        pthread_mutex_unlock(&dest->mutex);
        pthread_mutex_unlock(&buffer->mutex);

        sleep(1);
    }
    return NULL;
}

void *ThreadLocalizacao(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    ArquivoData *buffer = &arquivos[0];
    ArquivoData *dest = &arquivos[4];

    while (!t->stop) {
        pthread_mutex_lock(&buffer->mutex);
        pthread_mutex_lock(&dest->mutex);

        t->acessando[0] = true;
        t->acessando[4] = true;
        LogThread(t);
        moveLog(nomeArquivos[0], nomeArquivos[4], "Localizacao");
        t->acessando[0] = false;
        t->acessando[4] = false;

        pthread_mutex_unlock(&dest->mutex);
        pthread_mutex_unlock(&buffer->mutex);

        sleep(1);
    }
    return NULL;
}

void *ThreadPropaganda(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    ArquivoData *buffer = &arquivos[0];
    ArquivoData *dest = &arquivos[5];

    while (!t->stop) {
        pthread_mutex_lock(&buffer->mutex);
        pthread_mutex_lock(&dest->mutex);

        t->acessando[0] = true;
        t->acessando[5] = true;
        LogThread(t);
        moveLog(nomeArquivos[0], nomeArquivos[5], "Propaganda");
        t->acessando[0] = false;
        t->acessando[5] = false;

        pthread_mutex_unlock(&dest->mutex);
        pthread_mutex_unlock(&buffer->mutex);

        sleep(1);
    }
    return NULL;
}

void *ThreadCalculo(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    ArquivoData *buffer = &arquivos[0];
    ArquivoData *dest = &arquivos[6];

    while (!t->stop) {
        pthread_mutex_lock(&buffer->mutex);
        pthread_mutex_lock(&dest->mutex);

        t->acessando[0] = true;
        t->acessando[6] = true;
        LogThread(t);
        moveLog(nomeArquivos[0], nomeArquivos[6], "Calculo");
        t->acessando[0] = false;
        t->acessando[6] = false;

        pthread_mutex_unlock(&dest->mutex);
        pthread_mutex_unlock(&buffer->mutex);

        sleep(1);
    }
    return NULL;
}

void *ThreadOmega(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    while (!t->stop) {
        printf("[Omega] Rodando...\n");
        sleep(1);
    }
    return NULL;
}
void *ThreadKleubsMax(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    while (!t->stop) {
        printf("[KleubsMax] Rodando...\n");
        sleep(1);
    }
    return NULL;
}
void *ThreadChirpTome(void *arg) {
    ThreadData *t = (ThreadData *)arg;
    while (!t->stop) {
        printf("[ChirpTome] Rodando...\n");
        sleep(1);
    }
    return NULL;
}


///////////////////////
// Funções auxiliares
void geraLogs(const char *name) {
    FILE *f = fopen(name, "a");

    for (int i = 0; i < 5; i++) {
        fprintf(f, "%s\n", logs[rand() % nTipoLogs]);
    }

    fclose(f);
}
void moveLog(const char *origem, const char *destino, const char *palavra) {
    char tempName[100];
    strcpy(tempName, origem);
    strcat(tempName, "_temp.log");

    FILE *fin = fopen(origem, "r");
    FILE *fout = fopen(destino, "a");
    FILE *ftemp = fopen(tempName, "w");

    char linha[256];
    while (fgets(linha, sizeof(linha), fin)) {
        if (strstr(linha, palavra))
            fputs(linha, fout);
        else
            fputs(linha, ftemp);
    }

    fclose(fin);
    fclose(fout);
    fclose(ftemp);


    remove(origem);
    rename(tempName, origem);
}
void LogThread(ThreadData *t) {
    printf("Thread %d:");
    for(int i = 0; i < nArquivos; i++)
        printf(" %c:%d", nomeArquivos[i][0], t->acessando[i]);
    printf("\n");
}





















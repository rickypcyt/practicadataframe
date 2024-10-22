#include "lib.h"

Columna* crearColumna(const char* nombre, TipoDato tipo, int numFilas) {
    Columna* columna = (Columna*)malloc(sizeof(Columna));
    if (columna == NULL) {
        return NULL;
    }

    strncpy(columna->nombre, nombre, 29);
    columna->nombre[29] = '\0';
    columna->tipo = tipo;
    columna->numFilas = numFilas;

    columna->esNulo = (unsigned char*)calloc(numFilas, sizeof(unsigned char));
    if (columna->esNulo == NULL) {
        free(columna);
        return NULL;
    }

    switch (tipo) {
        case TEXTO:
            columna->datos = calloc(numFilas, sizeof(char*));
            break;
        case NUMERICO:
            columna->datos = calloc(numFilas, sizeof(double));
            break;
        case FECHA:
            columna->datos = calloc(numFilas, sizeof(Fecha));
            break;
    }

    if (columna->datos == NULL) {
        free(columna->esNulo);
        free(columna);
        return NULL;
    }

    return columna;
}

void eliminarColumna(Columna* columna) {
    if (columna == NULL) {
        return;
    }

    if (columna->tipo == TEXTO) {
        char** datos = (char**)columna->datos;
        for (int i = 0; i < columna->numFilas; i++) {
            free(datos[i]);
        }
    }

    free(columna->datos);
    free(columna->esNulo);
    free(columna);
}

Dataframe* crearDataframe() {
    Dataframe* df = (Dataframe*)malloc(sizeof(Dataframe));
    if (df == NULL) {
        return NULL;
    }

    df->columnas = NULL;
    df->numColumnas = 0;
    df->numFilas = 0;
    df->indice = NULL;

    return df;
}

void eliminarDataframe(Dataframe* df) {
    if (df == NULL) {
        return;
    }

    for (int i = 0; i < df->numColumnas; i++) {
        eliminarColumna(&df->columnas[i]);
    }

    free(df->columnas);
    free(df->indice);
    free(df);
}

Columna* buscarColumna(Dataframe* df, const char* nombreColumna) {
    if (df == NULL || nombreColumna == NULL) {
        return NULL;
    }

    for (int i = 0; i < df->numColumnas; i++) {
        if (strcmp(df->columnas[i].nombre, nombreColumna) == 0) {
            return &df->columnas[i];
        }
    }

    return NULL;
}

// Implementaciones de las demás funciones irían aquí...


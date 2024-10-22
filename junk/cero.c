#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>

#define MAX_LINE_LENGTH 1000
#define DELIMITER ","

// Función para eliminar espacios en blanco al inicio y final de una cadena
void trim(char *str) {
    char *start = str;
    char *end = str + strlen(str) - 1;

    while(isspace((unsigned char)*start)) start++;
    while(end > start && isspace((unsigned char)*end)) end--;

    *(end + 1) = '\0';
    memmove(str, start, end - start + 2);
}

void procesarCSV(const char* nombreArchivo) {
    FILE* archivo = fopen(nombreArchivo, "r");
    if (archivo == NULL) {
        printf("No se pudo abrir el archivo.\n");
        return;
    }

    char linea[MAX_LINE_LENGTH];
    int numeroFila = 0;

    while (fgets(linea, sizeof(linea), archivo) != NULL) {
        numeroFila++;
        printf("Fila %d: ", numeroFila);

        char* token = strtok(linea, DELIMITER);
        bool primerToken = true;
        while (token != NULL) {
            if (!primerToken) {
                printf(",");
            }
            trim(token);
            if (strlen(token) == 0 || strcmp(token, "NULL") == 0 || strcmp(token, "null") == 0) {
                printf("NULO");
            } else {
                printf("%s", token);
            }
            token = strtok(NULL, DELIMITER);
            primerToken = false;
        }
        printf("\n");
    }

    fclose(archivo);
}

int main() {
    const char* nombreArchivo = "datos.csv";
    procesarCSV(nombreArchivo);
    return 0;
}
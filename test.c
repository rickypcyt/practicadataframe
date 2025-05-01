#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
    FILE *archivo = fopen("datos.csv", "r");
    if (!archivo) {
        perror("Error al abrir el archivo");
        return 1;
    }

    char linea[1024];
    int filas = 0;
    int columnas = 0;

    while (fgets(linea, sizeof(linea), archivo)) {
        // Eliminar salto de línea
        size_t longitud = strlen(linea);
        if (longitud > 0 && linea[longitud - 1] == '\n') {
            linea[longitud - 1] = '\0';
            longitud--;
        }

        // Saltar líneas vacías
        if (longitud == 0) continue;

        filas++; // Contar filas no vacías

        // Dividir la línea en tokens (columnas) e imprimir
        char *token = strtok(linea, ",");
        int columnas_actual = 0;

        while (token != NULL) {
            if (filas == 1) { // Primera fila: contar columnas
                columnas++;
            }
            printf("%s\t", token); // Imprimir cada campo
            token = strtok(NULL, ",");
            columnas_actual++;
        }
        printf("\n");
    }

    fclose(archivo);

    printf("\nNúmero de filas: %d\n", filas);
    printf("Número de columnas: %d\n", columnas);

    return 0;
}

#ifndef LIB_H
#define LIB_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Colores para salida
#define BLK "\e[0;30m"
#define RED "\e[0;31m"
#define GRN "\e[0;32m"
#define YEL "\e[0;33m"
#define BLU "\e[0;34m"
#define MAG "\e[0;35m"
#define CYN "\e[0;36m"
#define WHT "\e[0;37m"

// Reset de color
#define reset "\e[0m"

// Tamaño máximo para la línea del archivo
#define MAX_LINE_LENGTH 1024

// Declaraciones de funciones
void comandos();
void leerCSV(const char *nombreArchivo);
void solicitarNombreArchivo(char *nombreArchivo, size_t size);
void crearDataframe();  // Puedes implementar esta función si es necesario

#endif // LIB_H

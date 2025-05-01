#include "lib.h"  // Incluir las funciones definidas en lib.h

int main() {
  // Mostrar estado de los ejercicios
  printf(GREEN "Estado de los ejercicios:\n" RESET);
  printf("Ejercicio 1: HECHO\n");
  printf("Ejercicio 2: HECHO\n");
  printf("Ejercicio 3: HECHO\n");
  printf("Ejercicio 4: HECHO\n");
  printf("Ejercicio 5: HECHO\n");
  printf("Ejercicio 6: HECHO\n");
  printf("Ejercicio 7: HECHO\n");
  printf("Ejercicio 8: HECHO\n");
  printf("Ejercicio 9: HECHO\n");
  printf("Ejercicio 10: HECHO\n\n");

  // Mostrar datos del alumno
  printf(GREEN "Datos del alumno:\n" RESET);
  printf("Nombre: Ricardo\n");
  printf("Apellidos: Perez\n");
  printf("Correo: ricardo.perez04@goumh.umh.es\n\n");

  inicializarLista(); // Inicializar la lista al inicio
  printf(GREEN "Sistema de Gestión de Dataframes\n" RESET);

  // Iniciar interfaz de línea de comandos interactiva
  CLI();
  liberarListaCompleta(&listaDF);
  return 0;
}

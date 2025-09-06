
## Programación Orientada a Objetos en el Juego de Cartas

Este proyecto utiliza los principales conceptos de la Programación Orientada a Objetos (POO) para modelar un juego de cartas español. A continuación se explica cómo se aplican estos conceptos en el código:

### Herencia

La herencia permite que una clase hija herede métodos y atributos de una clase padre. En este juego, tanto `Mazo` como `Mano` heredan de la clase `ColeccionDeCartas`. Gracias a esto, no es necesario volver a escribir métodos como `agregar_carta` o `__len__` en la clase `Mano`, ya que los "hereda" de su clase padre. Así, se evita la duplicación de código y se facilita el mantenimiento.

### Polimorfismo

El polimorfismo permite que diferentes objetos respondan a la misma función de manera específica. Por ejemplo, la función `print(jugador1)` funciona correctamente y muestra la mano del jugador porque la clase `Jugador` implementa el método especial `__str__`, que a su vez llama al `__str__` de su objeto `Mano`. Además, la función `len()` se puede usar tanto en un objeto `Mazo` como en un objeto `Mano` (`len(mazo_juego)` y `len(jugador1.mano)`), devolviendo el número de cartas en cada uno. Esto es posible porque ambas clases implementan el método `__len__`.

### Encapsulamiento

El encapsulamiento consiste en ocultar los detalles internos de una clase y exponer solo lo necesario. Aunque en este código no se usan atributos privados explícitos (con doble guion bajo `__`), el principio se aplica: los detalles de cómo un `Mazo` guarda sus cartas no le importan al `Jugador`. El jugador simplemente pide una carta usando `mazo.repartir_una()`, sin necesidad de saber si las cartas se almacenan en una lista, tupla u otra estructura.

### Composición

La composición es el principio de que una clase puede contener instancias de otras clases. En este juego, la clase `Jugador` no es una `Mano`, sino que tiene una `Mano`. Es decir, un jugador "tiene una" mano, en lugar de "ser una" mano. Esto permite construir objetos complejos a partir de otros más simples y reutilizables.

---

Estos conceptos hacen que el código sea más modular, reutilizable y fácil de entender, facilitando la extensión y el mantenimiento del juego.

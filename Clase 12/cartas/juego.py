from mazo import Mazo

# Creamos una instancia de la clase Mazo.
# Esto llama al __init__ de Mazo, que a su vez crea 40 instancias de Carta.
mi_mazo = Mazo()

# Usamos un método del objeto 'mi_mazo'.
mi_mazo.barajar()

# Repartimos una carta. 'carta_repartida' es ahora una instancia de Carta.
carta_repartida = mi_mazo.repartir_una()

# Gracias al método __str__ de la clase Carta, podemos imprimirla de forma legible.
print(f"La carta repartida es: {carta_repartida}")
print(f"Quedan {len(mi_mazo)} cartas en el mazo.")  
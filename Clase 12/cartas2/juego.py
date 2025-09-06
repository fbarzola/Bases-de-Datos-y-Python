from mazo import Mazo
from jugador import Jugador

def jugar_carta_mas_alta():
    """Función principal para ejecutar el juego."""
    print("--- 🃏 Iniciando el juego de 'La Carta Más Alta' ---")

    # 1. Crear y barajar el mazo (Instanciación de Mazo)
    mazo_juego = Mazo()
    mazo_juego.barajar()
    
    # 2. Crear jugadores (Instanciación de Jugador)
    jugador1 = Jugador("Ana")
    jugador2 = Jugador("Juan")
    
    print(f"\n¡{jugador1.nombre} y {jugador2.nombre} se preparan para jugar!")
    
    # 3. Cada jugador toma una carta
    print("\nRepartiendo cartas...")
    carta_j1 = jugador1.tomar_carta(mazo_juego)
    carta_j2 = jugador2.tomar_carta(mazo_juego)

    # El método __str__ de Jugador muestra su mano automáticamente
    print(jugador1)
    print(jugador2)

    # 4. Determinar el ganador
    print("\n--- 🏆 Resultado ---")
    if carta_j1.numero > carta_j2.numero:
        print(f"¡Gana {jugador1.nombre} con un {carta_j1.numero} contra un {carta_j2.numero}!")
    elif carta_j2.numero > carta_j1.numero:
        print(f"¡Gana {jugador2.nombre} con un {carta_j2.numero} contra un {carta_j1.numero}!")
    else:
        print(f"¡Es un empate con el número {carta_j1.numero}!")
        
    print(f"\nQuedan {len(mazo_juego)} cartas en el mazo.")

# --- Ejecutar el juego ---
if __name__ == "__main__":
    jugar_carta_mas_alta()
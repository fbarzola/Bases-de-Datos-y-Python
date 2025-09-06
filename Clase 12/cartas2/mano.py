from coleccion import ColeccionDeCartas
# --- CLASE HIJA: MANO ---
# Mano también hereda de ColeccionDeCartas.
class Mano(ColeccionDeCartas):
    """Representa la mano de un jugador. Hereda de ColeccionDeCartas."""
    # No necesita un __init__ propio, usará el de la clase padre.
    # Hereda automáticamente __len__, __str__ y agregar_carta.
    # ¡Reutilización de código en acción!
    pass
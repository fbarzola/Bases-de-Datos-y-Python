# --- CLASE CARTA ---
# Representa una única carta de la baraja.
# Este es nuestro "molde" para crear objetos de tipo carta.
class Carta:
    """
    Representa una carta individual de la baraja española.
    Cada carta tiene un palo y un número.
    """
    def __init__(self, palo: str, numero: int):
        # El método __init__ es el constructor. Se llama al crear una nueva instancia.
        # 'self' se refiere a la instancia específica que se está creando.
        self.palo = palo
        self.numero = numero

    def __str__(self) -> str:
        # Este "método mágico" define cómo se representa el objeto como un string.
        # Es útil para poder hacer print() de un objeto y ver algo legible.
        return f"{self.numero} de {self.palo}"

    def __repr__(self) -> str:
        # __repr__ es similar, pero se usa para una representación "oficial" del objeto.
        return self.__str__()

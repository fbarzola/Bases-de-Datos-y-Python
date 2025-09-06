class SuperClase:
    def __init__(self, valor):
        self.valor = valor

    def imprimir_valor(self):
        print(self.valor)

class SubClase(SuperClase):
    def __init__(self, valor, otro_valor):
        # Llama al inicializador de la superclase
        super().__init__(valor)  
        self.otro_valor = otro_valor

    def imprimir_valores(self):
        # Llama al método de la superclase
        super().imprimir_valor()  
        print(self.otro_valor)

# Crear una instancia de SubClase
objeto = SubClase(10, 20)
objeto.imprimir_valores()


class Ejemplo:
    atributo_clase = 10 # Atributo de clase

# Crear dos instancias de la clase Ejemplo
instancia1 = Ejemplo()
instancia2 = Ejemplo()

# Acceso al atributo de clase
print("Atributo de clase:", Ejemplo.atributo_clase) # Salida: 10

# Acceso al atributo de clase desde una instancia
print("Atributo de instancia instancia1:", instancia1.atributo_clase)  # Salida: 10
print("Atributo de instancia instancia2:", instancia2.atributo_clase)  # Salida: 10

# Modificar el atributo de clase a través de la clase
Ejemplo.atributo_clase = 20 
print("Atributo de clase modificado:", Ejemplo.atributo_clase) # Salida: 20
print("Atributo de instancia instancia1 modificado:", instancia1.atributo_clase)
print("Atributo de instancia instancia2 modificado:", instancia2.atributo_clase)

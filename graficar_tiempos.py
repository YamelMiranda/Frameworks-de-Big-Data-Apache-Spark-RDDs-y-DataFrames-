import matplotlib.pyplot as plt

# Tiempos que obtuviste (segundos), cámbialos por los reales
tiempo_rdd = 1.32
tiempo_df = 1.99

# Nombres para el eje X
metodos = ['RDD', 'DataFrame']
tiempos = [tiempo_rdd, tiempo_df]

plt.figure(figsize=(8, 5))
plt.bar(metodos, tiempos, color=['blue', 'green'])
plt.ylabel('Tiempo (segundos)')
plt.title('Comparación de tiempos Word Count en Spark')
plt.ylim(0, max(tiempos) + 5)

# Mostrar los valores encima de cada barra
for i, tiempo in enumerate(tiempos):
    plt.text(i, tiempo + 0.3, f'{tiempo:.2f}s', ha='center', fontsize=12)

plt.savefig("comparacion_tiempos.png")  # Guarda el gráfico como imagen
plt.show()  # Muestra la ventana con el gráfico



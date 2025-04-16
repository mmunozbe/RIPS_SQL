import sys
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QPushButton, QLineEdit, QTableWidget,
    QTableWidgetItem, QGridLayout, QScrollArea, QMessageBox, QProgressBar
)
from PyQt5.QtCore import QThread, pyqtSignal
from sqlalchemy import create_engine, inspect
import pandas as pd

# Imprimir versiones para depuración
print(f"pandas version: {pd.__version__}")
print(f"sqlalchemy version: {pd.__version__}")
try:
    import pyodbc
    print(f"pyodbc version: {pyodbc.version}")
except ImportError:
    print("pyodbc no está instalado")

# Clase para manejar la consulta en un hilo secundario
class QueryWorker(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(pd.DataFrame, str)
    error = pyqtSignal(str)

    def __init__(self, engine, query, page, page_size):
        super().__init__()
        self.engine = engine
        self.query = query
        self.page = page
        self.page_size = page_size

    def run(self):
        try:
            # Añadir paginación a la consulta
            offset = (self.page - 1) * self.page_size
            paginated_query = f"{self.query} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {self.page_size} ROWS ONLY"
            print(f"Ejecutando consulta paginada: {paginated_query}")

            # Usar chunks para procesar datos en lotes
            chunks = pd.read_sql(paginated_query, self.engine, chunksize=1000)
            df = pd.concat([chunk for chunk in chunks], ignore_index=True)
            df = df.fillna('')

            self.finished.emit(df, None)
        except Exception as e:
            self.error.emit(str(e))

# Función para obtener vistas o tablas
def get_items(engine, item_type):
    inspector = inspect(engine)
    try:
        if item_type == "Vistas":
            return inspector.get_view_names()
        else:
            return inspector.get_table_names()
    except Exception as e:
        print(f"Error al obtener {item_type.lower()}: {e}")
        return []

# Función para obtener columnas de una vista/tabla
def get_columns(engine, item):
    inspector = inspect(engine)
    try:
        return [col['name'] for col in inspector.get_columns(item, schema='dbo')]
    except Exception as e:
        print(f"Error al obtener columnas de {item}: {e}")
        return []

# Clase principal de la aplicación
class DatabaseSearcherApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Buscador de Vistas/Tablas - sxpintada")
        self.setGeometry(100, 100, 1000, 700)

        # Widget principal
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.layout = QVBoxLayout(self.central_widget)

        # Campos de conexión
        self.conn_layout = QGridLayout()
        self.server_entry = QLineEdit("localhost,1433")
        self.database_entry = QLineEdit("")
        self.username_entry = QLineEdit("sa")
        self.password_entry = QLineEdit("")
        self.password_entry.setEchoMode(QLineEdit.Password)
        self.conn_layout.addWidget(QLabel("Server,Port:"), 0, 0)
        self.conn_layout.addWidget(self.server_entry, 0, 1)
        self.conn_layout.addWidget(QLabel("Database:"), 1, 0)
        self.conn_layout.addWidget(self.database_entry, 1, 1)
        self.conn_layout.addWidget(QLabel("Username:"), 2, 0)
        self.conn_layout.addWidget(self.username_entry, 2, 1)
        self.conn_layout.addWidget(QLabel("Password:"), 3, 0)
        self.conn_layout.addWidget(self.password_entry, 3, 1)
        self.connect_button = QPushButton("Conectar")
        self.connect_button.clicked.connect(self.test_connection)
        self.conn_layout.addWidget(self.connect_button, 4, 0, 1, 2)
        self.layout.addLayout(self.conn_layout)

        # Selección de vistas o tablas
        self.type_layout = QHBoxLayout()
        self.type_label = QLabel("Seleccionar:")
        self.type_combo = QComboBox()
        self.type_combo.addItems(["Vistas", "Tablas"])
        self.type_combo.currentIndexChanged.connect(self.load_items)
        self.type_layout.addWidget(self.type_label)
        self.type_layout.addWidget(self.type_combo)
        self.layout.addLayout(self.type_layout)

        # Dropdown para vistas/tablas
        self.view_layout = QHBoxLayout()
        self.view_label = QLabel("Selecciona una vista/tabla:")
        self.items = []
        self.view_dropdown = QComboBox()
        self.view_dropdown.setEditable(True)
        self.view_dropdown.currentIndexChanged.connect(self.load_columns)
        self.view_layout.addWidget(self.view_label)
        self.view_layout.addWidget(self.view_dropdown)
        self.layout.addLayout(self.view_layout)

        # Área desplazable para filtros
        self.filter_scroll = QScrollArea()
        self.filter_widget = QWidget()
        self.filter_layout = QGridLayout(self.filter_widget)
        self.filter_entries = []
        self.columns = []
        self.filter_scroll.setWidget(self.filter_widget)
        self.filter_scroll.setWidgetResizable(True)
        self.layout.addWidget(self.filter_scroll)

        # Botón para buscar
        self.search_button = QPushButton("Buscar")
        self.search_button.clicked.connect(self.run_query)
        self.search_button.setEnabled(False)
        self.layout.addWidget(self.search_button)

        # Controles de paginación
        self.pagination_layout = QHBoxLayout()
        self.page_label = QLabel("Página: 1")
        self.prev_button = QPushButton("Anterior")
        self.next_button = QPushButton("Siguiente")
        self.prev_button.clicked.connect(self.prev_page)
        self.next_button.clicked.connect(self.next_page)
        self.prev_button.setEnabled(False)
        self.next_button.setEnabled(False)
        self.pagination_layout.addWidget(self.page_label)
        self.pagination_layout.addWidget(self.prev_button)
        self.pagination_layout.addWidget(self.next_button)
        self.layout.addLayout(self.pagination_layout)

        # Barra de progreso
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.layout.addWidget(self.progress_bar)

        # Etiqueta de depuración
        self.debug_label = QLabel("Esperando conexión...")
        self.debug_label.setWordWrap(True)
        self.layout.addWidget(self.debug_label)

        # Tabla para resultados
        self.table = QTableWidget()
        self.table.setRowCount(0)
        self.table.setColumnCount(0)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setAlternatingRowColors(True)
        self.layout.addWidget(self.table)

        self.engine = None
        self.current_page = 1
        self.page_size = 1000  # Número de filas por página
        self.current_query = ""
        self.current_columns = []
        self.has_more_data = False

    def test_connection(self):
        server = self.server_entry.text()
        database = self.database_entry.text()
        username = self.username_entry.text()
        password = self.password_entry.text()

        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{server}/{database}"
            "?driver=ODBC+Driver+18+for+SQL+Server"
            "&Encrypt=no"
            "&TrustServerCertificate=yes"
        )

        try:
            self.engine = create_engine(connection_string, connect_args={'connect_timeout': 10})
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()  # Prueba de conexión
            print(f"Tablas encontradas: {tables}")
            self.debug_label.setText("Conexión exitosa")
            self.search_button.setEnabled(True)
            self.load_items()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error de conexión: {str(e)}")
            self.debug_label.setText(f"Error de conexión: {str(e)}")
            self.search_button.setEnabled(False)

    def load_items(self):
        if not self.engine:
            return

        self.view_dropdown.clear()
        try:
            self.items = get_items(self.engine, self.type_combo.currentText())
            self.view_dropdown.addItems(self.items)
            self.debug_label.setText("Vistas/Tablas cargadas")
        except Exception as e:
            self.debug_label.setText(f"Error al cargar vistas/tablas: {e}")

    def load_columns(self):
        for i in reversed(range(self.filter_layout.count())):
            widget = self.filter_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        self.filter_entries = []

        selected_item = self.view_dropdown.currentText()
        if not selected_item:
            return

        try:
            self.columns = get_columns(self.engine, selected_item)
            print(f"Columnas de {selected_item}: {self.columns}")
        except Exception as e:
            print(f"Error al obtener columnas de {selected_item}: {e}")
            self.columns = []

        self.table.setRowCount(0)
        self.table.setColumnCount(0)

        for i, col in enumerate(self.columns):
            label = QLabel(f"{col}:")
            entry = QLineEdit()
            self.filter_layout.addWidget(label, i, 0)
            self.filter_layout.addWidget(entry, i, 1)
            self.filter_entries.append(entry)

        self.debug_label.setText("Filtros actualizados")

    def run_query(self):
        selected_item = self.view_dropdown.currentText()
        if not selected_item:
            self.table.setRowCount(1)
            self.table.setColumnCount(1)
            self.table.setHorizontalHeaderLabels(["Mensaje"])
            self.table.setItem(0, 0, QTableWidgetItem("Por favor, selecciona una vista/tabla."))
            self.debug_label.setText("No se seleccionó vista/tabla")
            return

        filters = {col: entry.text() for col, entry in zip(self.columns, self.filter_entries)}
        print(f"Filtros aplicados: {filters}")

        # Construir la consulta base
        self.current_columns = self.columns
        query = f"SELECT {', '.join(f'[{col}]' for col in self.current_columns)} FROM [{selected_item}]"

        # Agregar filtros
        conditions = []
        for col, value in filters.items():
            if value and value.strip() and col in self.current_columns:
                safe_value = value.strip().replace("'", "''")
                conditions.append(f"[{col}] LIKE '%{safe_value}%'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        self.current_query = query
        self.current_page = 1
        self.load_page()

    def load_page(self):
        self.search_button.setEnabled(False)
        self.prev_button.setEnabled(False)
        self.next_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.debug_label.setText("Cargando datos...")

        # Crear el hilo para ejecutar la consulta
        self.worker = QueryWorker(self.engine, self.current_query, self.current_page, self.page_size)
        self.worker.finished.connect(self.on_query_finished)
        self.worker.error.connect(self.on_query_error)
        self.worker.start()

    def on_query_finished(self, df, error):
        self.table.setRowCount(0)
        self.table.setColumnCount(0)

        if error:
            self.table.setRowCount(1)
            self.table.setColumnCount(1)
            self.table.setHorizontalHeaderLabels(["Mensaje"])
            self.table.setItem(0, 0, QTableWidgetItem(f"Error: {error}"))
            self.debug_label.setText(f"Error: {error}")
        elif df.empty:
            self.table.setRowCount(1)
            self.table.setColumnCount(1)
            self.table.setHorizontalHeaderLabels(["Mensaje"])
            self.table.setItem(0, 0, QTableWidgetItem("No se encontraron resultados."))
            self.debug_label.setText("No se encontraron resultados")
            self.has_more_data = False
        else:
            self.table.setColumnCount(len(self.current_columns))
            self.table.setHorizontalHeaderLabels(self.current_columns)
            self.table.setRowCount(len(df))

            print(f"Insertando {len(df)} filas en la tabla")
            for i, (_, row) in enumerate(df.iterrows()):
                values = [str(val) for val in row]
                print(f"Fila {i}: {values}")
                for col_idx, value in enumerate(values):
                    self.table.setItem(i, col_idx, QTableWidgetItem(value))
            self.debug_label.setText(f"Insertadas {len(df)} filas (Página {self.current_page})")

            # Determinar si hay más datos
            self.has_more_data = len(df) == self.page_size

        self.table.resizeColumnsToContents()
        self.page_label.setText(f"Página: {self.current_page}")
        self.prev_button.setEnabled(self.current_page > 1)
        self.next_button.setEnabled(self.has_more_data)
        self.search_button.setEnabled(True)
        self.progress_bar.setVisible(False)

    def on_query_error(self, error):
        self.table.setRowCount(1)
        self.table.setColumnCount(1)
        self.table.setHorizontalHeaderLabels(["Mensaje"])
        self.table.setItem(0, 0, QTableWidgetItem(f"Error: {error}"))
        self.debug_label.setText(f"Error: {error}")
        self.search_button.setEnabled(True)
        self.progress_bar.setVisible(False)

    def prev_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.load_page()

    def next_page(self):
        if self.has_more_data:
            self.current_page += 1
            self.load_page()

# Ejecutar aplicación
if __name__ == "__main__":
    try:
        app = QApplication(sys.argv)
        window = DatabaseSearcherApp()
        window.show()
        sys.exit(app.exec_())
    except Exception as e:
        print(f"Error al iniciar la aplicación: {e}")
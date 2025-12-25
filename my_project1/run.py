from app1 import (create_api1,
                  create_api2,
                  create_api3,
                  create_api4)
import threading


def start_api(app, port):
    app.run(debug=True, port=port, use_reloader=False)


def main():
    app1 = create_api1()
    app2 = create_api2()
    app3 = create_api3()
    app4 = create_api4()


    apis = [(app1, 5001),
            (app2, 5002),
            (app3, 5003),
            (app4, 5004)]
    

    for app, port in apis:
        threading.Thread(target=start_api,
                          args=(app, port),
                          daemon=True).start()

    input('APIs rodando. Pressione ENTER para sair.\n')
    
if __name__ == '__main__':
    main()

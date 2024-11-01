import { Component } from '@angular/core';
import { ServiceService } from './services/service.service';
import { cita_interface } from './interface/cita.interface';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})

export class AppComponent {
  title = 'redis_citas';

  departamentos:any=[]
  codigo_departamento:string='';
  codigo_provincia:string='';
  provincias:any=[];
  distritos:any=[];
  datos_precalentados:any;

  horario:any;
  lista_citas:any;
  tramites = [
    {opcion:'Medicina General'}, 
    {opcion: 'Traumatología'}, 
    {opcion: 'Psicologa'},
    {opcion: 'Laboratorio'},
    {opcion: 'Odontología'}
  ];
  opcionSeleccionada: string = ''; 
  mensaje:any;
  mensajitus:string='';
  //hora:any;

  cita: cita_interface={
    nombre: '',
    apellido_paterno:'',
    apellido_materno:'',
    departamento:'',
    provincia:'',
    distrito:'',
    tramite:''
  };

  hora_seleccionada:string = '';

  constructor(private citaService: ServiceService) {
    //this.citaService.get_inicializar()
    console.log('Inicia servicio citaService');
//ojo quitare

    

    
  }

   ngOnInit(){
    this.consultarHorarios();
    this.consultarCitas();
    //this.precalentado(); //add
    this.citaService.get_departamentos().subscribe(
      data => {
        console.log('Inicia servicio citaService.get_departamentos ',data);
        this.departamentos = data;
        console.log('Inicia servicio citaService.get_departamentos this.departamentos ',this.departamentos);
      },
      error => {
        console.error('Error en proceso cita', error);
      }
    );
   }    

   consultarHorarios(){
    console.log("INICIO consultarHorarios ");
    this.citaService.getEstadoHorarios().subscribe(
      data => {
        this.horario = data;
        console.log("this.horario ",this.horario);
      },
      error => {
        console.error('Error al obtener el horario', error);
      }
    );
   }

   consultarCitas(){
    this.citaService.getCitas().subscribe(
      data => {
        this.lista_citas = data;
      },
      error => {
        console.error('Error al obtener el horario', error);
      }
    );
   }

   onRadioChange(event: any) {
    this.consultarHorarios();
  }

  cuenta_regresiva: number = 11; // Inicia el cuenta_regresiva en 10 segundos
  private cuenta_regresivaTimeout: any;

  estado_registrado:boolean=false;
  iniciar_cuenta_regresiva(): void {
    this.cuenta_regresivaTimeout = setTimeout(() => {
      this.cuenta_regresiva--;
      if (this.cuenta_regresiva > 0 && this.estado_registrado==false) {
        // Si el cuenta_regresiva no ha llegado a cero, inicia el próximo setTimeout
        this.iniciar_cuenta_regresiva();
      } else if(this.estado_registrado==false){
        this.consultarHorarios()
        this.mensajitus='Hora desbloqueada, por favor elija otra hora nuevamente';
      }
    }, 1000); // Cada segundo (1000 milisegundos)
  }


  proceso_cita(hora:any){
    this.estado_registrado=false;
    this.hora_seleccionada=hora;
    this.citaService.proceso_cita(hora).subscribe(
      data => {
        this.mensaje = data;

        this.consultarHorarios();
        this.cuenta_regresiva=11;
        this.iniciar_cuenta_regresiva()
        this.mensajitus='Hora '+hora+' bloqueada por: ';
      },
      error => {
        console.error('Error en proceso cita', error);
      }
    );

  }

  registrar_cita(){
    this.estado_registrado=true;
    this.citaService.agregar_cita(this.hora_seleccionada, this.cita).subscribe(
      (response) => {
       this.cuenta_regresiva=0;
       this.consultarCitas();
       this.consultarHorarios();
       clearTimeout(this.cuenta_regresivaTimeout);
       this.mensajitus='Cita agregada con éxito';
       this.limpiar_campos();
      },
      (error) => {
        console.error('Error:', error);
      }
    );
  }

  cancelar_cita(){
    this.limpiar_campos();
  }
  
  limpiar_campos(){
    this.cita={
      nombre: '',
      apellido_paterno:'',
      apellido_materno:'',
      departamento:'',
      provincia:'',
      distrito:'',
      tramite:''
    };
  }
  onDepartamentoChange(event: any): void {
    const departamentoSeleccionado = event.target.value;
    const departamento_busqueda = this.departamentos.find(
      (      dep: { valor: any; }) => dep.valor === departamentoSeleccionado
    );
    console.log("departamento_busqueda ",departamento_busqueda);

    this.codigo_departamento= departamento_busqueda.llave.split("_")[1];
    console.log("this.codigo_departamento ",this.codigo_departamento);
    
    this.citaService.get_provincias(this.codigo_departamento).subscribe(
      data => {
        this.provincias = data;
        console.log("this.provincias ",this.provincias);
      },
      error => {
        console.error('Error en proceso cita', error);
        
      }
    );
  }

  onProvinciaChange(event: any): void {
    const provinciaSeleccionado = event.target.value;
    const provincia_busqueda = this.provincias.find(
      (      prov: { valor: any; }) => prov.valor === provinciaSeleccionado
    );
    this.codigo_provincia= provincia_busqueda.llave.split("_")[1]
    this.citaService.get_distritos(this.codigo_provincia).subscribe(
      data => {
        this.distritos = data;
      },
      error => {
        console.error('Error en proceso cita', error);
      }
    );
  }

  /*
  inicializar_horario(){
    this.citaService.get_inicializar().subscribe(
      data => {
        this.lista_citas = data;
      },
      error => {
        console.error('Error al obtener el horario', error);
      }
    );
   }
/*
  precalentado(){
    console.log('===============llamando precalentado===================');
    this.citaService.get_precarga_datos().subscribe(
      data => {
        this.datos_precalentados = data;
        console.log('Error en proceso cita', this.datos_precalentados);
      },
      error => {
        console.error('Error al obtener el horario', error);
      }
    );
   }*/
   
}

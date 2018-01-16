'use strict'

/*

*/

function getDataset() {
  return new Promise((resolve, reject) => {
    const http = require('http')
    const urlDataset = 'http://www.datos.gov.co/resource/xax6-k7eu.json'
    const querySELECT = '?$SELECT=codigomunicipio,codigoestablecimiento,nombreestablecimiento,direccion,telefono,codigo_etc,niveles,jornada,grados,numero_de_sedes,prestador_de_servicio,propiedad_planta_fisica,calendario,estrato_socio_economico,correo_electronico'
    const queryWHERE = '&$WHERE=codigomunicipio=8001 AND (prestador_de_servicio="OFICIAL" OR prestador_de_servicio="CONCESION")'
    const url = urlDataset + querySELECT + queryWHERE
    http.get(url, (res) => {
      const { statusCode } = res
      if (statusCode !== 200) {
        reject(`Falló el Request. Status Code: ${statusCode}`)
      }
      res.setEncoding('utf8')
      let rawData = ''
      res.on('data', (chunk) => { rawData += chunk })
      res.on('end', () => {
        const response = JSON.parse(rawData)
        if (response) {
          resolve(response)
        } else {
          reject(`Falló la construcción del Dataset. Es posible que el Endpoint haya cambiado o el query presenta error. QUERY= ${querySELECT + queryWHERE}`)
        }
      })
    }).on('error', (e) => {
      reject(`Falló el Query a la url: (${urlDataset}...), el sitio está fuera de servicio o hay problemas de conexión a internet. No se puede obtener Dataset.`)
    })
  })
}

function normalize(dataset) {
  return new Promise((resolve, reject) => {

    for(let school of dataset){

      let re, temp, array
      if(school.estrato_socio_economico){
        re = /\D/g
        temp = school.estrato_socio_economico.replace(re, '')
        array = temp.split('')
        school.estrato_socio_economico = array
      }else {
        school.estrato_socio_economico = 'none'        
      }

      array = school.grados.split(',')
      school.grados = array

      array = school.jornada.split(',')
      school.jornada = array

      array = school.niveles.split(',')
      school.niveles = array

      if(school.telefono){
        re = ' '
        temp = school.telefono.replace(re, '')
        array = temp.split('-')
        school.telefono = array
      }else {
        school.telefono = 'none'
      }

    }

    resolve(dataset)
  })
}

function convertAddress(dataset){
  return new Promise((resolve, reject) => {
    const abrev = [
      'AU',
      'AV',
      'AC',
      'AK',
      'BL',
      'CL',
      'KR',
      'CT',
      'CQ',
      'CV',
      'CC',
      'DG',
      'PJ',
      'PS',
      'PT',
      'TV',
      'TC',
      'VT',
      'VI',
      'AP',
      'BR',
      'BQ',
      'BG',
      'CS',
      'CD',
      'CO',
      'ED',
      'EQ',
      'ET',
      'KM',
      'LC',
      'LM',
      'LT',
      'MZ',
      'MN',
      'OF',
      'PQ',
      'PA',
      'PI',
      'PL',
      'PD',
      'SC',
      'SM',
      'TZ',
      'TO',
      'UL',
      'UR',
      'ZN'
    ]
    const items = [
      'Autopista',
      'Avenida',
      'Avenida Calle',
      'Avenida Carrera',
      'Bulevar',
      'Calle',
      'Carrera',
      'Carretera',
      'Circular',
      'Circunvalar',
      'Cuentas Corridas',
      'Diagonal',
      'Pasaje',
      'Paseo',
      'Peatonal',
      'Transversal',
      'Troncal',
      'Variante',
      'Vía',
      'Apartamento',
      'Barrio',
      'Bloque',
      'Bodega',
      'Casa',
      'Ciudadela',
      'Conjunto Residencial',
      'Edificio',
      'Esquina',
      'Etapa',
      'Kilómetro',
      'Local',
      'Local Mezzanine',
      'Lote',
      'Manzana',
      'Mezzanine',
      'Oficina',
      'Parque',
      'Parqueadero',
      'Piso',
      'Planta',
      'Predio',
      'Sector',
      'Supermanzana',
      'Terraza',
      'Torre',
      'Unidad Residencial',
      'Urbanización',
      'Zona'
    ]
    const cantSchool = dataset.length - 1
    for(let i = 0; i <= cantSchool; i++) {
      let address = dataset[i].direccion
      address = address.replace(/\#/, '')
      address = address.replace(/\-\s/, '')
      address = address.replace(/\./, '')
      const re = /\s/
      let arrayDir = address.split(re)
      let stdDir = arrayDir[0]
    
      let j = 0
      while (abrev[j] !== stdDir) {
        if (j === abrev.length) {
          j = 0
          arrayDir.splice(0, 1)
          stdDir = arrayDir[0]
        } else {
          j = j + 1
        }
      }
      stdDir = items[j] + ' ' + arrayDir[1]
      j = 2
      if (isNaN(arrayDir[j])) {
        stdDir += arrayDir[j]
        j = j + 1
      }
      stdDir += ' #' + arrayDir[j]
      j = j + 1
      if (isNaN(arrayDir[j])) {
        stdDir += arrayDir[j]
        j = j + 1
      }
      stdDir += '-' + arrayDir[j]
      dataset[i].direccion = stdDir
    }
    resolve(dataset)
  })
}

/*
Esta función se conecta con el API de Geocodificacion de Google...
Recibe como parámetros, la dirección de acuerdo con el estandar de nomenclatura del país, la ciudad, el departamento y el país...
Devuelve un objeto JSON de la siguiente forma
{ location: { lat: 10.9444572, lng: -74.77351279999999 },
  viewport:
   { northeast: { lat: 10.9458023302915, lng: -74.77215526970848 },
     southwest: { lat: 10.9431043697085, lng: -74.7748532302915  } },
  place_id: 'EjJDbC4gMTdFICM2Yi0zOSwgQmFycmFucXVpbGxhLCBBdGzDoW50aWNvLCBDb2xvbWJpYQ' }
Si hay un error en la consulta devuelve un JSON así:
{ result: 'failed' }
*/


let index = 0
function geocoder (dataset) {
  return new Promise((resolve, reject) => {
    const APIKeyGoogleMaps = require('./APIKeyGoogleMaps')
    const googleMapsClient = require('@google/maps').createClient({
      key: APIKeyGoogleMaps.APIKey,
      Promise: Promise
    })
    const query = {
      address: dataset[index].direccion,
      components: {
        administrative_area_level_1: 'Colombia',
        administrative_area_level_2: 'Atlántico',
        locality: 'Barranquilla'
      }
    }
    console.log('x')
    googleMapsClient.geocode(query).asPromise()
    .then((response) => {
      const geolocation = {
        location: response.json.results[0].geometry.location,
        viewport: response.json.results[0].geometry.viewport,
        place_id: response.json.results[0].place_id
      }
      dataset[index].geolocation = geolocation
      if (index == (dataset.length - 1)){
        resolve(dataset)
      }else{
        console.log(dataset[index].geolocation)
        index = index + 1
        return(geocoder(dataset))
      }
    })
    .catch((err) => {
      reject(err)
    })    
  })
}


function error(message) {
  console.log(message)
}

function showDataset(dataset) {
  console.log(dataset)
}

getDataset()
  .then(normalize)
  .then(convertAddress)
  .then(geocoder)
  .then(showDataset)
  .catch(error)

getDataset()

module.exports = {
  getDataset
}

export async function getProductCatalog({enterprise="", family="", subfamily="", brand="", catalog="", description="", page=1}) {

  const url = new URL('/getProductCatalog', window.location.origin)

  if (enterprise) {
    url.searchParams.set('enterprise', enterprise)
  }

  if (family) {
    url.searchParams.set('family', family)
  }

  if (subfamily) {
    url.searchParams.set('subfamily', subfamily)
  }

  if (brand) {
    url.searchParams.set('brand', brand)
  }

  if (catalog) {
    url.searchParams.set('catalog', catalog)
  }

  if (description) {
    url.searchParams.set('description', description)
  }

  url.searchParams.set('page', page.toString())
  url.searchParams.set('per_page', '10')

  let reqObj = null
  try {
    reqObj = await fetch(url)
    
    if (!reqObj.ok) {
      return 'Hubo un error'
    }
  } catch(err){
      return 'Hubo un error'
  }

  let jsonObj = null
  try {
    jsonObj = await reqObj.json()
    return jsonObj 
  } catch (err) {
    return 'La respuesta esta en formato incorrecto'
  }
}

export async function uploadCatalogExcel(){}
const excelFrom = document.getElementById('load-excel-form')

excelFrom.addEventListener('submit',(e) => {
  e.preventDefault()
  const formData = new FormData(excelFrom)
  const file = formData.get('excel-file')
  console.log(file)
})

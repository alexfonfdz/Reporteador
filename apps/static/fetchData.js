async function fetchYears(){
    try{
        const response = await fetch('/getYears',{
            method: 'GET',
        });
        const json = await response.json();
        return json; 
    } catch (error) {
        console.error('Error fetching years:', error);
        throw error;
    }
}

async function fetchMonths(year){
    try{
        const response = await fetch('/getMonths',{
            method: 'POST',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                year: year,
              }),
        });
        const json = await response.json();
        return json; 
    } catch (error) {
    console.error('Error fetching months::', error);
    throw error;
    }
}

async function fetchDays(year, month){
    try{
        const response = await fetch('/getDays',{
            method: 'POST',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                year: year,
                month: month
              }),
        });
        const json = await response.json();
        return json; 
    } catch (error) {
    console.error('Error fetching months::', error);
    throw error;
    }
}

async function fetchProducts(day){
    try{
        const response = await fetch('/getProducts',{
            method: 'POST',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                day: day
              }),
        });
        const json = await response.json();
        return json; 
    } catch (error) {
    console.error('Error fetching months::', error);
    throw error;
    }
}

async function fetchFolio(day, product){
    try{
        const response = await fetch('/getFolio',{
            method: 'POST',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                day: day,
                product: product
              }),
        });
        const json = await response.json();
        return json; 
    } catch (error) {
    console.error('Error fetching months::', error);
    throw error;
    }
}
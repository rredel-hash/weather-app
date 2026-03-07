const AppState={
 stationId:"390051",
 chart:null,
 daily:null
};

document.addEventListener("DOMContentLoaded",init);

async function init(){

 const res=await fetch("data/stations.json");
 const stations=await res.json();

 const sel=document.getElementById("stationSelect");

 stations.forEach(s=>{
   const o=document.createElement("option");
   o.value=s.id;
   o.textContent=s.name;
   sel.appendChild(o);
 });

 sel.onchange=e=>changeStation(e.target.value);

 changeStation(AppState.stationId);
}

async function changeStation(id){

 AppState.stationId=id;

 const r = await fetch(`data/${id}/current.json`);
 const d=await r.json();

 tempNow.textContent=d.temp_now;
 timestamp.textContent=d.timestamp;
 tminToday.textContent=d.tmin_today;
 tmaxToday.textContent=d.tmax_today;
 precipToday.textContent=d.precip_today;
}

async function openTempChart(){

 if(!AppState.daily){
   const r=await fetch(
     `../data/${AppState.stationId}/daily_5d.json`
   );
   AppState.daily=await r.json();
 }

 const labels=AppState.daily.days.map(d=>d.date);
 const tmax=AppState.daily.days.map(d=>d.tmax);

 if(AppState.chart)
   AppState.chart.destroy();

 AppState.chart=new Chart(
   document.getElementById("tempChart"),
   {
     type:"bar",
     data:{
       labels:labels,
       datasets:[{label:"Tmax",data:tmax}]
     }
   }
 );
}



import { useEffect, useState } from 'react';

export default function useWs(url){
  const [events, setEvents] = useState([]);
  useEffect(()=>{
    const ws = new WebSocket(url);
    ws.onmessage = (ev)=>{
      try{
        const msg = JSON.parse(ev.data);
        setEvents(prev => [...prev.slice(-499), msg]);
      }catch{}
    };
    return ()=>ws.close();
  }, [url]);
  return events;
}

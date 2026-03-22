import json
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from langchain_ollama import OllamaLLM
from tavily import TavilyClient

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- กองทัพ Agent ---
researcher_llm = OllamaLLM(model="llama3.1", temperature=0.1)
pro_llm = OllamaLLM(model="llama3.2", temperature=0.1)       
con_llm = OllamaLLM(model="mistral", temperature=0.1)        
critic_llm = OllamaLLM(model="qwen2.5:1.5b", temperature=0) 
judge_llm = OllamaLLM(model="llama3.1", temperature=0.1)      

tavily = TavilyClient(api_key="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")

# --- โครงสร้างรับข้อมูลสำหรับการคัดค้าน ---
class ReconsiderRequest(BaseModel):
    topic: str
    previous_summary: str
    user_argument: str

def fast_research(query: str):
    try:
        response = tavily.search(query=query, search_depth="advanced", max_results=3)
        context = "\n".join([r["content"] for r in response["results"]])
        sources = [{"title": r.get("title", "Source"), "url": r["url"]} for r in response["results"]]
        return context, sources
    except: return "ไม่มีข้อมูล", []

class DebateRequest(BaseModel):
    topic: str

@app.post("/debate")
async def start_advanced_council(request: DebateRequest):
    topic = request.topic
    async def run_pipeline():
        yield f"data: {json.dumps({'step': 1, 'status': 'Researcher กำลังขุดข้อมูลล่าสุด...'})}\n\n"
        raw_facts, sources = fast_research(f"วิเคราะห์ข้อดีและข้อเสียของ {topic}")
        yield f"data: {json.dumps({'research': 'รวบรวมข้อมูลเสร็จสิ้น', 'sources': sources, 'step': 2})}\n\n"

        yield f"data: {json.dumps({'status': 'ฝ่ายสนับสนุนกำลังเสนอความเห็น...'})}\n\n"
        pro_prompt = f"หัวข้อ: {topic}\nข้อมูล: {raw_facts[:500]}\nสรุปข้อดีมา 1 ประโยค (ไม่เกิน 15 คำ) ตอบเป็นภาษาไทยเท่านั้น ห้ามตอบเป็นภาษาอังกฤษ"
        pro_msg = pro_llm.invoke(pro_prompt).strip()
        yield f"data: {json.dumps({'pro': pro_msg})}\n\n"

        yield f"data: {json.dumps({'status': 'ฝ่ายค้านกำลังหาช่องโหว่...'})}\n\n"
        con_prompt = f"ฝ่ายหนุนเพิ่งพูดว่า: '{pro_msg}'\nจากข้อมูล: {raw_facts[:500]}\nจงเถียงกลับเจ็บๆ 1 ประโยค (ไม่เกิน 20 คำ) ห้ามทำ SWOT ตอบเป็นภาษาไทยเท่านั้น ห้ามตอบเป็นภาษาอังกฤษ"
        con_msg = con_llm.invoke(con_prompt).strip()
        yield f"data: {json.dumps({'con': con_msg, 'step': 3})}\n\n"
        await asyncio.sleep(0.3)

        yield f"data: {json.dumps({'status': 'Critic กำลังตรวจสอบความสมเหตุสมผล...'})}\n\n"
        critic_prompt = f"วิจารณ์การดีเบตเรื่อง {topic} ระหว่าง '{pro_msg}' กับ '{con_msg}' สั้นๆ 1 ประโยค ตอบเป็นภาษาไทยเท่านั้น ห้ามตอบเป็นภาษาอังกฤษ"
        critic_msg = critic_llm.invoke(critic_prompt).strip()
        yield f"data: {json.dumps({'status': f'Critic: {critic_msg}', 'critic': critic_msg})}\n\n"
        await asyncio.sleep(0.8)

        yield f"data: {json.dumps({'status': 'ประธานสภากำลังประมวลมติสุดท้าย...'})}\n\n"
        judge_prompt = f"หัวข้อ: {topic}\nฝ่ายหนุน: {pro_msg}\nฝ่ายค้าน: {con_msg}\nบทวิจารณ์: {critic_msg}\nตัดสินผู้ชนะ (หนุน/ค้าน) และเหตุผล 1 ประโยค พร้อมคะแนน (0-100) ตอบรูปแบบ: ผู้ชนะ: [หนุน/ค้าน] | มติ: [คำตัดสิน] | คะแนน: [ตัวเลข]"
        raw_result = judge_llm.invoke(judge_prompt).strip()
        
        winner, summary, score = "หนุน", raw_result, 50
        try:
            parts = raw_result.split("|")
            for p in parts:
                if "ผู้ชนะ" in p: winner = "ค้าน" if "ค้าน" in p else "หนุน"
                if "มติ" in p: summary = p.split(":")[1].strip()
                if "คะแนน" in p: score = int(''.join(filter(str.isdigit, p)))
        except: pass
        yield f"data: {json.dumps({'summary': summary, 'score': score, 'winner': winner, 'step': 4, 'status': 'ปิดการประชุม'})}\n\n"

    return StreamingResponse(run_pipeline(), media_type="text/event-stream")

# --- ใหม่: Endpoint สำหรับพิจารณามติใหม่เมื่อโดนคัดค้าน ---
@app.post("/reconsider")
async def reconsider_verdict(request: ReconsiderRequest):
    reconsider_prompt = f"""
    [ประธานสภา: รับคำร้องคัดค้านจากมนุษย์]
    หัวข้อ: {request.topic}
    มติเดิม: {request.previous_summary}
    มนุษย์คัดค้านว่า: "{request.user_argument}"
    
    งานของคุณ:
    1. วิเคราะห์เหตุผลของมนุษย์ว่ามีน้ำหนักพอจะเปลี่ยนมติไหม
    2. ตัดสินใจใหม่ (ยืนยันคำเดิม หรือ เปลี่ยนข้างก็ได้)
    3. ตอบกลับสั้นๆ 1 ประโยค พร้อมคะแนนใหม่
    รูปแบบการตอบ: ผู้ชนะ: [หนุน/ค้าน] | มติ: [คำตัดสินใหม่] | คะแนน: [ตัวเลข]
    """
    raw_result = judge_llm.invoke(reconsider_prompt).strip()
    
    winner, summary, score = "หนุน", raw_result, 50
    try:
        parts = raw_result.split("|")
        for p in parts:
            if "ผู้ชนะ" in p: winner = "ค้าน" if "ค้าน" in p else "หนุน"
            if "มติ" in p: summary = p.split(":")[1].strip()
            if "คะแนน" in p: score = int(''.join(filter(str.isdigit, p)))
    except: pass
    
    return {"summary": summary, "score": score, "winner": winner}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
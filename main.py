def gpt_score_job(job):
    try:
        prompt = f"""
You are an AI assistant evaluating job opportunities.

Rate the following job on a scale of 1 to 10 based on:
- Relevance to 'Data Science', Machine Learning, AI, Analytics
- Seniority of the role
- Remote flexibility
- Salary competitiveness

Respond in this format:
Score: X/10
Reason: [short reason]

Job Title: {job.get('job_title')}
Company: {job.get('company_name')}
Summary: {job.get('job_summary')}
Location: {job.get('job_location')}
Salary: {job.get('base_salary')}
Description: {job.get('job_summary')}
        """
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        text = response.choices[0].message['content']
        score_line = next((line for line in text.splitlines() if "Score" in line), "Score: 0/10")
        score = int(score_line.split(":")[1].split("/")[0].strip())
        return score, text
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, f"OpenAI error: {e}"

def push_to_airtable(job, base_score, base_reason):
    try:
        table = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

        # Base lightweight fields
        fields = {
            "job_title": job.get("job_title"),
            "company_name": job.get("company_name"),
            "job_location": job.get("job_location"),
            "job_summary": job.get("job_summary"),
            "job_function": job.get("job_function"),
            "job_industries": job.get("job_industries"),
            "job_base_pay_range": job.get("job_base_pay_range"),
            "url": job.get("url"),
            "job_posted_time": job.get("job_posted_time"),
            "Score": base_score,
            "Reason": base_reason,
        }

        poster = job.get("job_poster")
        if isinstance(poster, str) and len(poster.strip()) > 0 and len(poster.strip()) <= 255:
            fields["job_poster"] = poster.strip()

        num_applicants = job.get("job_num_applicants")
        if num_applicants and isinstance(num_applicants, (int, float, str)):
            try:
                fields["job_num_applicants"] = int(float(num_applicants))
            except ValueError:
                pass

        posted_date = job.get("job_posted_date")
        if posted_date:
            fields["job_posted_date"] = posted_date

        base_salary = job.get("base_salary")
        if base_salary:
            fields["base_salary"] = base_salary

        # === Create the Airtable record first ===
        created_record = table.create(fields)
        record_id = created_record['id']
        logging.info(f"✅ Added to Airtable: {job.get('job_title')} at {job.get('company_name')}")

        # === Now fetch GPT score separately ===
        gpt_score, gpt_reason = gpt_score_job(job)

        # === PATCH the record with GPT score ===
        table.update(record_id, {
            "GPT Score": gpt_score,
            "GPT Reason": gpt_reason
        })
        logging.info(f"🧠 Updated GPT Score: {gpt_score}/10")

    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

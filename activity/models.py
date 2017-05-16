from django.db import models


class List(models.Model):
    list_id = models.CharField(max_length=20, primary_key=True)  # mailchimp
    name = models.CharField(max_length=255)
    member_count = models.IntegerField(blank=True)
    unsubscribe_count = models.IntegerField(blank=True)
    # recipient_count = models.IntegerField(blank=True)

    rlid = models.IntegerField(blank=True, null=True)
    src = models.CharField(max_length=1)  # m for mailchimp, r for responsys

    class Meta:
        unique_together = ('list_id', 'src')


class Customer(models.Model):
    # use RIID for Responsys
    customer_id = models.CharField(max_length=20, blank=True, null=True)
    new_customer_id = models.CharField(max_length=20, blank=True, null=True)
    list = models.ForeignKey(List, db_index=True)
    timestamp_opt = models.DateTimeField(blank=True, null=True)
    last_changed = models.DateTimeField(blank=True, null=True)
    email = models.EmailField(db_index=True)
    status = models.CharField(max_length=1, null=True)
    avg_open = models.FloatField(null=True)
    avg_click = models.FloatField(null=True)

    RIID = models.IntegerField(blank=True, null=True, db_index=True)  # Responsys ID

    src = models.CharField(max_length=1, null=True)  # m for mailchimp, r for responsys
    segment = models.CharField(max_length=20, null=True)

    class Meta:
        unique_together = ('list', 'email')


class Campaign(models.Model):
    campaign_id = models.CharField(max_length=30)
    list = models.ForeignKey(List)
    send_time = models.DateTimeField(blank=True, null=True)
    emails_sent = models.IntegerField(blank=True, null=True)
    # campaign_target = models.CharField(max_length=10)

    opens = models.IntegerField(blank=True, null=True)
    clicks = models.IntegerField(blank=True, null=True)
    unique_opens = models.IntegerField(blank=True, null=True)
    unique_clicks = models.IntegerField(blank=True, null=True)
    open_rate = models.FloatField(blank=True, null=True)
    click_rate = models.FloatField(blank=True, null=True)

    src = models.CharField(max_length=1)  # m for mailchimp, r for responsys

    class Meta:
        unique_together = ('campaign_id', 'list')


class Activity(models.Model):
    campaign = models.ForeignKey(Campaign)
    #customer = models.ForeignKey(Customer)
    customer_id = models.CharField(max_length=20, blank=True, null=True)
    list = models.ForeignKey(List)
    action = models.IntegerField()
    RIID = models.IntegerField(blank=True, null=True, db_index=True)  # Responsys ID
    timestamp = models.DateTimeField()
    ip = models.CharField(max_length=100, blank=True)
    device = models.CharField(blank=True, max_length=100)
    url = models.URLField(max_length=500, blank=True, null=True)
    email = models.EmailField(db_index=True)
    email_format = models.CharField(max_length=1, blank=True)
    timestamp_complaint = models.DateTimeField(blank=True, null=True)
    reason = models.CharField(max_length=100, blank=True)
    source = models.CharField(max_length=100, blank=True)

    src = models.CharField(max_length=1)  # m for mailchimp, r for responsys

    #class Meta:
        #unique_together = ('campaign', 'list', 'customer_id', 'action', 'timestamp')


class ResponsysFile(models.Model):
    name = models.CharField(max_length=100, primary_key=True)
    is_processed = models.BooleanField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)


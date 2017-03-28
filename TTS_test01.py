# import pyttsx
# # engine = pyttsx.init()
# # engine.say('Greetings!')
# # engine.say('How are you today?')
# # engine.runAndWait()
#
# speech_engine = pyttsx.init('sapi5')
# voices = speech_engine.getProperty('voices')
#
# # speech_engine = pyttsx.init('sapi5') # see http://pyttsx.readthedocs.org/en/latest/engine.html#pyttsx.init
# speech_engine.setProperty('rate', 150)
#
#
#
# def speak(text):
# 	speech_engine.say(text)
# 	speech_engine.runAndWait()
#
# speak("hello greeteong!")


import pyttsx

engine = pyttsx.init('sapi5')
voices = engine.getProperty('voices')
engine.setProperty('voice', voices[1].id) #change index to change voices
engine.setProperty('rate', 150)
engine.say('안녕하세요. 전 보이스웨어 담당자인 유미라고 합니다.')
engine.runAndWait()
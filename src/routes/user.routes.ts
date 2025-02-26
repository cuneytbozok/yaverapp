import { Router } from 'express';
import { UserRepository } from '../repositories/UserRepository';
import { auth, AuthRequest } from '../middleware/auth.middleware';

const router = Router();
const userRepository = new UserRepository();

// Protected route - Get current user
router.get('/login', auth, async (req: AuthRequest, res) => {
  try {
    const user = await userRepository.findById(req.user?.id as string);
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }
    // Don't send the password in the response
    const { password, ...userWithoutPassword } = user;
    res.json(userWithoutPassword);
  } catch (error) {
    res.status(400).json({ error: (error as Error).message });
  }
});

router.post('/', async (req, res) => {
  try {
    const user = await userRepository.create(req.body);
    res.status(201).json(user);
  } catch (error) {
    res.status(400).json({ error: (error as Error).message });
  }
});

router.get('/:id', async (req, res) => {
  try {
    const user = await userRepository.findById(req.params.id);
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }
    res.json(user);
  } catch (error) {
    res.status(400).json({ error: (error as Error).message });
  }
});

export default router; 